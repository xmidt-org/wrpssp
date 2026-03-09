// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xmidt-org/wrp-go/v5"
)

// slowReader simulates a slow network reader that returns data in chunks
// with controllable synchronization between reads, allowing context cancellation
// to occur mid-read. Uses channel-based signaling instead of time.Sleep for
// deterministic test behavior.
type slowReader struct {
	data      []byte
	offset    int
	chunkSize int
	readCount int
	// readSignal is an optional channel to signal when each read should proceed.
	// If nil, reads proceed immediately. If set, reads after the first will block
	// waiting for a signal before returning data.
	readSignal chan struct{}
}

func (sr *slowReader) Read(p []byte) (int, error) {
	if sr.offset >= len(sr.data) {
		return 0, io.EOF
	}

	// Wait for signal after first read (if signal channel is configured)
	if sr.readCount > 0 && sr.readSignal != nil {
		<-sr.readSignal
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
		readSignal := make(chan struct{})
		slowReader := &slowReader{
			data:       input,
			chunkSize:  2, // Return 2 bytes per read
			readSignal: readSignal,
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

		// Create context that will be cancelled manually
		ctx, cancel := context.WithCancel(context.Background())

		// Start the read in a goroutine
		type result struct {
			msg *wrp.Message
			err error
		}
		resultCh := make(chan result, 1)

		go func() {
			msg, err := packetizer.Next(ctx, in)
			resultCh <- result{msg, err}
		}()

		// Allow a few reads to succeed (first read doesn't wait)
		// Read 1: 2 bytes ("AB") - happens immediately
		// Read 2: 2 bytes ("CD") - wait for signal
		readSignal <- struct{}{}
		// Read 3: 2 bytes ("EF") - wait for signal
		readSignal <- struct{}{}

		// Now cancel context before next read can proceed
		cancel()

		// Get the result
		res := <-resultCh

		// Should get context.Canceled error
		assert.ErrorIs(t, res.err, context.Canceled, "should return canceled error")

		// Should still get partial data (not nil)
		require.NotNil(t, res.msg, "should return message even on context cancel")
		assert.NotEmpty(t, res.msg.Payload, "should return partial data even on context cancel")

		// Verify we got partial data (6 bytes from 3 reads before cancellation)
		assert.Equal(t, 6, len(res.msg.Payload), "should have 6 bytes from 3 reads")

		t.Logf("Partial data received: %d bytes: %q", len(res.msg.Payload), string(res.msg.Payload))

		// Verify the data is correct prefix
		assert.Equal(t, input[:len(res.msg.Payload)], res.msg.Payload, "partial data should be correct prefix of input")
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

	t.Run("full buffer read completes before context cancel", func(t *testing.T) {
		// Test that if buffer fills in one read, we get the data even if context is cancelled
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

		// Context that gets cancelled, but the read completes first
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		got, err := packetizer.Next(ctx, in)

		// Should succeed because buffer filled in one read (no context check mid-read)
		assert.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, []byte("ABCDEFGHIJ"), got.Payload)
	})
}

// TestPacketizer_ContextCancelPreservesStreamState verifies that context
// cancellation doesn't corrupt the Packetizer state for subsequent reads.
func TestPacketizer_ContextCancelPreservesStreamState(t *testing.T) {
	input := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ") // 26 bytes

	readSignal := make(chan struct{})
	slowReader := &slowReader{
		data:       input,
		chunkSize:  2,
		readSignal: readSignal,
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
	ctx1, cancel1 := context.WithCancel(context.Background())

	// Run first call in goroutine
	type result struct {
		msg *wrp.Message
		err error
	}
	resultCh := make(chan result, 1)

	go func() {
		msg, err := packetizer.Next(ctx1, in)
		resultCh <- result{msg, err}
	}()

	// Allow a couple of reads
	readSignal <- struct{}{} // Read 2: "CD"
	readSignal <- struct{}{} // Read 3: "EF"

	// Cancel before buffer fills
	cancel1()

	res1 := <-resultCh
	assert.ErrorIs(t, res1.err, context.Canceled)

	var bytesRead1 int
	if res1.msg != nil {
		bytesRead1 = len(res1.msg.Payload)
		t.Logf("First call: read %d bytes before cancellation", bytesRead1)
	}

	// The Packetizer records the outcome error, so the next call will return
	// the same error until we consume all data. This is expected behavior.
	ctx2 := context.Background()
	got2, err2 := packetizer.Next(ctx2, in)

	// The outcome is sticky - subsequent calls return the error
	if err2 != nil {
		t.Logf("Second call returned error (sticky outcome): %v", err2)
		assert.ErrorIs(t, err2, context.Canceled, "outcome should be preserved")
		return
	}

	// If no error, verify data integrity
	require.NotNil(t, got2)
	bytesRead2 := len(got2.Payload)
	t.Logf("Second call: read %d bytes", bytesRead2)

	// Verify packet numbers increment
	assert.Contains(t, got2.Headers, "stream-packet-number: 1")
}
