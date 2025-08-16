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
	consecutivePunctuationEndCount := 0

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

			// Heuristic check for 3 consecutive punctuation ends
			if !isThought && textChunk != "" {
				trimmedChunk := strings.TrimSpace(textChunk)
				if len(trimmedChunk) > 0 {
					const punctuations = "。？！.\"?'”’"
					lastChar := string([]rune(trimmedChunk)[len([]rune(trimmedChunk))-1])

					if strings.Contains(punctuations, lastChar) {
						consecutivePunctuationEndCount++
						logger.LogDebug(fmt.Sprintf("Chunk ended with punctuation. Consecutive count: %d", consecutivePunctuationEndCount))
					} else {
						consecutivePunctuationEndCount = 0
					}
				} else {
					consecutivePunctuationEndCount = 0
				}
			} else {
				// Reset if it's a thought or an empty text chunk
				consecutivePunctuationEndCount = 0
			}

			if consecutivePunctuationEndCount >= 3 {
				logger.LogInfo("Stream ending due to 3 consecutive chunks ending with punctuation.")
				// The current line has already been written, so we just break
				cleanExit = true
				break
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

				// Check for empty response - if we have STOP but no accumulated text at all, it's incomplete
				if len(trimmedText) == 0 {
					logger.LogError("Finish reason 'STOP' with no text content detected. This indicates an empty response. Triggering retry.")
					interruptionReason = "FINISH_EMPTY_RESPONSE"
					needsRetry = true
				} else if !strings.HasSuffix(trimmedText, "[done]") {
					runes := []rune(trimmedText)
					lastChar := string(runes[len(runes)-1])
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
			isEndOfResponse := finishReason == "STOP" || finishReason == "MAX_TOKENS"
			processedLine := RemoveDoneTokenFromLine(line, isEndOfResponse)

			if _, err := writer.Write([]byte(processedLine + "\n\n")); err != nil {
				return fmt.Errorf("failed to write to output stream: %w", err)
			}

			// Flush the response to ensure data is sent immediately to the client
			if flusher, ok := writer.(http.Flusher); ok {
				flusher.Flush()
			}

			if textChunk != "" && !isThought {
				isOutputtingFormalText = true
				accumulatedText += textChunk
				textInThisStream += textChunk
			}

			// Check for total output character limit
			if len(accumulatedText) >= 65535 {
				logger.LogInfo("Total output character limit (65535) reached. Treating as a clean exit.")
				cleanExit = true
				break
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

			// Flush the error response to ensure it's sent immediately
			if flusher, ok := writer.(http.Flusher); ok {
				flusher.Flush()
			}

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

			// Flush the error response to ensure it's sent immediately
			if flusher, ok := writer.(http.Flusher); ok {
				flusher.Flush()
			}

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
