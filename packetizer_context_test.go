// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xmidt-org/wrp-go/v5"
)

// slowReader simulates a slow network reader that returns data in chunks
// with delays between reads, allowing context cancellation to occur mid-read.
type slowReader struct {
	data      []byte
	offset    int
	chunkSize int
	delay     time.Duration
	readCount int
}

func (sr *slowReader) Read(p []byte) (int, error) {
	if sr.offset >= len(sr.data) {
		return 0, io.EOF
	}

	// Delay after first read to allow context cancellation
	if sr.readCount > 0 && sr.delay > 0 {
		time.Sleep(sr.delay)
	}
	sr.readCount++

	// Return data in small chunks
	chunkSize := sr.chunkSize
	if chunkSize == 0 {
		chunkSize = 1 // Default: one byte at a time
	}

	remaining := len(sr.data) - sr.offset
	toRead := chunkSize
	if toRead > remaining {
		toRead = remaining
	}
	if toRead > len(p) {
		toRead = len(p)
	}

	n := copy(p, sr.data[sr.offset:sr.offset+toRead])
	sr.offset += n

	return n, nil
}

// TestPacketizer_ContextCancelDuringRead specifically tests line 147 in readChunk()
// where context cancellation is checked mid-loop after partial data has been read.
func TestPacketizer_ContextCancelDuringRead(t *testing.T) {
	t.Run("context cancelled after partial read returns partial data", func(t *testing.T) {
		// Setup: Create a slow reader that will trigger context cancellation mid-read
		input := []byte("ABCDEFGHIJKLMNOP") // 16 bytes
		slowReader := &slowReader{
			data:      input,
			chunkSize: 2,                     // Return 2 bytes per read
			delay:     40 * time.Millisecond, // Delay between reads
		}

		packetizer, err := New(
			ID("slow-test"),
			Reader(slowReader),
			MaxPacketSize(20), // Want 20 bytes, but will cancel mid-read
			WithEncoding(EncodingIdentity),
		)
		require.NoError(t, err)

		in := wrp.Message{
			Type:        wrp.SimpleEventMessageType,
			Source:      "mac:112233445566",
			Destination: "event:test",
		}

		// Create context that will be cancelled after 90ms
		// Timeline:
		// 0ms: Read 2 bytes ("AB")
		// 40ms: Read 2 bytes ("CD") - total 4 bytes
		// 80ms: Read 2 bytes ("EF") - total 6 bytes
		// 90ms: Context timeout!
		// 120ms: Would read next, but context cancelled
		// Result: Check context at line 147, return 6 bytes with error
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Millisecond)
		defer cancel()

		got, err := packetizer.Next(ctx, in)

		// Should get context.DeadlineExceeded error
		assert.ErrorIs(t, err, context.DeadlineExceeded, "should return deadline exceeded error")

		// Should still get partial data (not nil)
		require.NotNil(t, got, "should return message even on context cancel")
		assert.NotEmpty(t, got.Payload, "should return partial data even on context cancel")

		// Verify we got partial data (not the full 20 bytes)
		assert.Greater(t, len(got.Payload), 0, "should have read at least some data")
		assert.Less(t, len(got.Payload), 20, "should have less than full packet size")

		t.Logf("Partial data received: %d bytes: %q (expected 4-8 bytes)", len(got.Payload), string(got.Payload))

		// Verify the data is correct prefix
		assert.Equal(t, input[:len(got.Payload)], got.Payload, "partial data should be correct prefix of input")
	})

	t.Run("context cancelled immediately returns empty data", func(t *testing.T) {
		// Test edge case: context already cancelled before any read
		input := []byte("ABCDEFGHIJKLMNOP")

		packetizer, err := New(
			ID("immediate-cancel"),
			Reader(&slowReader{data: input, chunkSize: 5}),
			MaxPacketSize(10),
			WithEncoding(EncodingIdentity),
		)
		require.NoError(t, err)

		in := wrp.Message{
			Type:        wrp.SimpleEventMessageType,
			Source:      "mac:112233445566",
			Destination: "event:test",
		}

		// Cancel context immediately
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		got, err := packetizer.Next(ctx, in)

		// Should get cancellation error
		assert.ErrorIs(t, err, context.Canceled)

		// Should get nil (no data read yet)
		assert.Nil(t, got)
	})

	t.Run("context cancelled after full buffer returns full data with no error", func(t *testing.T) {
		// Test that if buffer fills before context check, we get the data
		input := []byte("ABCDEFGHIJ") // Exactly 10 bytes

		packetizer, err := New(
			ID("full-buffer"),
			Reader(&slowReader{data: input, chunkSize: 10}), // Returns all data in one read
			MaxPacketSize(10),
			WithEncoding(EncodingIdentity),
		)
		require.NoError(t, err)

		in := wrp.Message{
			Type:        wrp.SimpleEventMessageType,
			Source:      "mac:112233445566",
			Destination: "event:test",
		}

		// Context with short timeout, but data arrives fast
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		got, err := packetizer.Next(ctx, in)

		// Should succeed because buffer filled in one read
		assert.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, []byte("ABCDEFGHIJ"), got.Payload)
	})
}

// TestPacketizer_ContextCancelPreservesStreamState verifies that context
// cancellation doesn't corrupt the Packetizer state for subsequent reads.
func TestPacketizer_ContextCancelPreservesStreamState(t *testing.T) {
	input := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ") // 26 bytes

	slowReader := &slowReader{
		data:      input,
		chunkSize: 2,
		delay:     30 * time.Millisecond,
	}

	packetizer, err := New(
		ID("state-test"),
		Reader(slowReader),
		MaxPacketSize(10),
		WithEncoding(EncodingIdentity),
	)
	require.NoError(t, err)

	in := wrp.Message{
		Type:        wrp.SimpleEventMessageType,
		Source:      "mac:112233445566",
		Destination: "event:test",
	}

	// First call: cancel after partial read
	ctx1, cancel1 := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel1()

	got1, err1 := packetizer.Next(ctx1, in)
	assert.ErrorIs(t, err1, context.DeadlineExceeded)

	var bytesRead1 int
	if got1 != nil {
		bytesRead1 = len(got1.Payload)
		t.Logf("First call: read %d bytes before cancellation", bytesRead1)
	}

	// The Packetizer records the outcome error, so the next call will return
	// the same error until we consume all data. This is expected behavior.
	ctx2 := context.Background()
	got2, err2 := packetizer.Next(ctx2, in)

	// The outcome is sticky - subsequent calls return the error
	if err2 != nil {
		t.Logf("Second call returned error (sticky outcome): %v", err2)
		assert.ErrorIs(t, err2, context.DeadlineExceeded, "outcome should be preserved")
		return
	}

	// If no error, verify data integrity
	require.NotNil(t, got2)
	bytesRead2 := len(got2.Payload)
	t.Logf("Second call: read %d bytes", bytesRead2)

	// Verify packet numbers increment
	assert.Contains(t, got2.Headers, "stream-packet-number: 1")
}
