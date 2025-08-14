package streaming

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"gemini-antiblock/config"
	"gemini-antiblock/logger"
)

var nonRetryableStatuses = map[int]bool{
	400: true, 401: true, 403: true, 404: true, 429: true,
}

// BuildRetryRequestBody builds a new request body for retry with accumulated context
func BuildRetryRequestBody(originalBody map[string]interface{}, accumulatedText string) map[string]interface{} {
	logger.LogDebug(fmt.Sprintf("Building retry request body. Accumulated text length: %d", len(accumulatedText)))
	logger.LogDebug(fmt.Sprintf("Accumulated text preview: %s", func() string {
		if len(accumulatedText) > 200 {
			return accumulatedText[:200] + "..."
		}
		return accumulatedText
	}()))

	retryBody := make(map[string]interface{})
	for k, v := range originalBody {
		retryBody[k] = v
	}

	contents, ok := retryBody["contents"].([]interface{})
	if !ok {
		contents = []interface{}{}
	}

	// Find last user message index
	lastUserIndex := -1
	for i := len(contents) - 1; i >= 0; i-- {
		if content, ok := contents[i].(map[string]interface{}); ok {
			if role, ok := content["role"].(string); ok && role == "user" {
				lastUserIndex = i
				break
			}
		}
	}

	// Build retry context
	history := []interface{}{
		map[string]interface{}{
			"role": "model",
			"parts": []interface{}{
				map[string]interface{}{"text": accumulatedText},
			},
		},
		map[string]interface{}{
			"role": "user",
			"parts": []interface{}{
				map[string]interface{}{"text": "Continue exactly where you left off without any preamble or repetition."},
			},
		},
	}

	// Insert history after last user message
	if lastUserIndex != -1 {
		newContents := make([]interface{}, 0, len(contents)+2)
		newContents = append(newContents, contents[:lastUserIndex+1]...)
		newContents = append(newContents, history...)
		newContents = append(newContents, contents[lastUserIndex+1:]...)
		retryBody["contents"] = newContents
		logger.LogDebug(fmt.Sprintf("Inserted retry context after user message at index %d", lastUserIndex))
	} else {
		newContents := append(contents, history...)
		retryBody["contents"] = newContents
		logger.LogDebug("Appended retry context to end of conversation")
	}

	logger.LogDebug(fmt.Sprintf("Final retry request has %d messages", len(retryBody["contents"].([]interface{}))))
	return retryBody
}

// ProcessStreamAndRetryInternally handles streaming with internal retry logic
func ProcessStreamAndRetryInternally(cfg *config.Config, initialReader io.Reader, writer io.Writer, originalRequestBody map[string]interface{}, upstreamURL string, originalHeaders http.Header) error {
	var accumulatedText string
	consecutiveRetryCount := 0
	currentReader := initialReader
	totalLinesProcessed := 0
	sessionStartTime := time.Now()

	isOutputtingFormalText := false
	swallowModeActive := false

	logger.LogInfo(fmt.Sprintf("Starting stream processing session. Max retries: %d", cfg.MaxConsecutiveRetries))

	for {
		interruptionReason := ""
		cleanExit := false
		streamStartTime := time.Now()
		linesInThisStream := 0
		textInThisStream := ""

		logger.LogDebug(fmt.Sprintf("=== Starting stream attempt %d/%d ===", consecutiveRetryCount+1, cfg.MaxConsecutiveRetries+1))

		// Create channel for SSE lines
		lineCh := make(chan string, 100)
		go SSELineIterator(currentReader, lineCh)

		// Process lines
		for line := range lineCh {
			totalLinesProcessed++
			linesInThisStream++

			var textChunk string
			var isThought bool

			if IsDataLine(line) {
				content := ParseLineContent(line)
				textChunk = content.Text
				isThought = content.IsThought
			}

			// Thought swallowing logic
			if swallowModeActive {
				if isThought {
					logger.LogDebug("Swallowing thought chunk due to post-retry filter:", line)
					finishReason := ExtractFinishReason(line)
					if finishReason != "" {
						logger.LogError(fmt.Sprintf("Stream stopped with reason '%s' while swallowing a 'thought' chunk. Triggering retry.", finishReason))
						interruptionReason = "FINISH_DURING_THOUGHT"
						break
					}
					continue
				} else {
					logger.LogInfo("First formal text chunk received after swallowing. Resuming normal stream.")
					swallowModeActive = false
				}
			}

			// Retry decision logic
			finishReason := ExtractFinishReason(line)
			needsRetry := false

			if finishReason != "" && isThought {
				logger.LogError(fmt.Sprintf("Stream stopped with reason '%s' on a 'thought' chunk. This is an invalid state. Triggering retry.", finishReason))
				interruptionReason = "FINISH_DURING_THOUGHT"
				needsRetry = true
			} else if IsBlockedLine(line) {
				logger.LogError(fmt.Sprintf("Content blocked detected in line: %s", line))
				interruptionReason = "BLOCK"
				needsRetry = true
			} else if finishReason == "STOP" {
				tempAccumulatedText := accumulatedText + textChunk
				trimmedText := strings.TrimSpace(tempAccumulatedText)
				if len(trimmedText) > 0 && !strings.HasSuffix(trimmedText, "[done]") {
					lastChar := trimmedText[len(trimmedText)-1:]
					logger.LogError(fmt.Sprintf("Finish reason 'STOP' treated as incomplete because text ends with '%s'. Triggering retry.", lastChar))
					interruptionReason = "FINISH_INCOMPLETE"
					needsRetry = true
				}
			} else if finishReason != "" && finishReason != "MAX_TOKENS" && finishReason != "STOP" {
				logger.LogError(fmt.Sprintf("Abnormal finish reason: %s. Triggering retry.", finishReason))
				interruptionReason = "FINISH_ABNORMAL"
				needsRetry = true
			}

			if needsRetry {
				break
			}

			// Line is good: forward and update state
			// Always attempt to remove the [done] token, as it should only appear at the very end.
			// This is more robust than only checking on the final message.
			processedLine := RemoveDoneTokenFromLine(line, true)

			if _, err := writer.Write([]byte(processedLine + "\n\n")); err != nil {
				return fmt.Errorf("failed to write to output stream: %w", err)
			}

			if textChunk != "" && !isThought {
				isOutputtingFormalText = true
				accumulatedText += textChunk
				textInThisStream += textChunk
			}

			if finishReason == "STOP" || finishReason == "MAX_TOKENS" {
				logger.LogInfo(fmt.Sprintf("Finish reason '%s' accepted as final. Stream complete.", finishReason))
				cleanExit = true
				break
			}
		}

		if !cleanExit && interruptionReason == "" {
			logger.LogError("Stream ended without finish reason - detected as DROP")
			interruptionReason = "DROP"
		}

		streamDuration := time.Since(streamStartTime)
		logger.LogDebug("Stream attempt summary:")
		logger.LogDebug(fmt.Sprintf("  Duration: %v", streamDuration))
		logger.LogDebug(fmt.Sprintf("  Lines processed: %d", linesInThisStream))
		logger.LogDebug(fmt.Sprintf("  Text generated this stream: %d chars", len(textInThisStream)))
		logger.LogDebug(fmt.Sprintf("  Total accumulated text: %d chars", len(accumulatedText)))

		if cleanExit {
			sessionDuration := time.Since(sessionStartTime)
			logger.LogInfo("=== STREAM COMPLETED SUCCESSFULLY ===")
			logger.LogInfo(fmt.Sprintf("Total session duration: %v", sessionDuration))
			logger.LogInfo(fmt.Sprintf("Total lines processed: %d", totalLinesProcessed))
			logger.LogInfo(fmt.Sprintf("Total text generated: %d characters", len(accumulatedText)))
			logger.LogInfo(fmt.Sprintf("Total retries needed: %d", consecutiveRetryCount))
			return nil
		}

		// Interruption & Retry Activation
		logger.LogError("=== STREAM INTERRUPTED ===")
		logger.LogError(fmt.Sprintf("Reason: %s", interruptionReason))

		if cfg.SwallowThoughtsAfterRetry && isOutputtingFormalText {
			logger.LogInfo("Retry triggered after formal text output. Will swallow subsequent thought chunks until formal text resumes.")
			swallowModeActive = true
		}

		logger.LogError(fmt.Sprintf("Current retry count: %d", consecutiveRetryCount))
		logger.LogError(fmt.Sprintf("Max retries allowed: %d", cfg.MaxConsecutiveRetries))
		logger.LogError(fmt.Sprintf("Text accumulated so far: %d characters", len(accumulatedText)))

		if consecutiveRetryCount >= cfg.MaxConsecutiveRetries {
			errorPayload := map[string]interface{}{
				"error": map[string]interface{}{
					"code":    504,
					"status":  "DEADLINE_EXCEEDED",
					"message": fmt.Sprintf("Retry limit (%d) exceeded after stream interruption. Last reason: %s.", cfg.MaxConsecutiveRetries, interruptionReason),
					"details": []interface{}{
						map[string]interface{}{
							"@type":                  "proxy.debug",
							"accumulated_text_chars": len(accumulatedText),
						},
					},
				},
			}

			errorBytes, _ := json.Marshal(errorPayload)
			writer.Write([]byte(fmt.Sprintf("event: error\ndata: %s\n\n", string(errorBytes))))
			return fmt.Errorf("retry limit exceeded")
		}

		consecutiveRetryCount++
		logger.LogInfo(fmt.Sprintf("=== STARTING RETRY %d/%d ===", consecutiveRetryCount, cfg.MaxConsecutiveRetries))

		// Build retry request
		retryBody := BuildRetryRequestBody(originalRequestBody, accumulatedText)
		retryBodyBytes, err := json.Marshal(retryBody)
		if err != nil {
			logger.LogError("Failed to marshal retry body:", err)
			time.Sleep(cfg.RetryDelayMs)
			continue
		}

		// Create retry request
		retryReq, err := http.NewRequest("POST", upstreamURL, bytes.NewReader(retryBodyBytes))
		if err != nil {
			logger.LogError("Failed to create retry request:", err)
			time.Sleep(cfg.RetryDelayMs)
			continue
		}

		// Copy headers
		for name, values := range originalHeaders {
			if name == "Authorization" || name == "X-Goog-Api-Key" || name == "Content-Type" || name == "Accept" {
				for _, value := range values {
					retryReq.Header.Add(name, value)
				}
			}
		}

		logger.LogDebug(fmt.Sprintf("Making retry request to: %s", upstreamURL))
		logger.LogDebug(fmt.Sprintf("Retry request body size: %d bytes", len(retryBodyBytes)))

		// Make retry request
		client := &http.Client{}
		retryResponse, err := client.Do(retryReq)
		if err != nil {
			logger.LogError(fmt.Sprintf("=== RETRY ATTEMPT %d FAILED ===", consecutiveRetryCount))
			logger.LogError("Exception during retry:", err)
			logger.LogError(fmt.Sprintf("Will wait %v before next attempt (if any)", cfg.RetryDelayMs))
			time.Sleep(cfg.RetryDelayMs)
			continue
		}

		logger.LogInfo(fmt.Sprintf("Retry request completed. Status: %d %s", retryResponse.StatusCode, retryResponse.Status))

		if nonRetryableStatuses[retryResponse.StatusCode] {
			logger.LogError("=== FATAL ERROR DURING RETRY ===")
			logger.LogError(fmt.Sprintf("Received non-retryable status %d during retry attempt %d", retryResponse.StatusCode, consecutiveRetryCount))

			// Write SSE error from upstream
			errorBytes, _ := io.ReadAll(retryResponse.Body)
			retryResponse.Body.Close()

			writer.Write([]byte(fmt.Sprintf("event: error\ndata: %s\n\n", string(errorBytes))))
			return fmt.Errorf("non-retryable error: %d", retryResponse.StatusCode)
		}

		if retryResponse.StatusCode != http.StatusOK {
			logger.LogError(fmt.Sprintf("Retry attempt %d failed with status %d", consecutiveRetryCount, retryResponse.StatusCode))
			logger.LogError("This is considered a retryable error - will try again if retries remain")
			retryResponse.Body.Close()
			time.Sleep(cfg.RetryDelayMs)
			continue
		}

		logger.LogInfo(fmt.Sprintf("✓ Retry attempt %d successful - got new stream", consecutiveRetryCount))
		logger.LogInfo(fmt.Sprintf("Continuing with accumulated context (%d chars)", len(accumulatedText)))

		currentReader = retryResponse.Body
	}
}
