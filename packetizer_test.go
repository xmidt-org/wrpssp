// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xmidt-org/wrp-go/v5"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name     string
		opts     []Option
		expected *Packetizer
		err      error
	}{
		{
			name: "valid options",
			opts: []Option{
				ID("123"),
				Reader(bytes.NewReader([]byte("HelloWorld"))),
				EstimatedLength(10),
				MaxPacketSize(5),
			},
			expected: &Packetizer{
				id:            "123",
				estimatedSize: 10,
				maxPacketSize: 5,
			},
			err: nil,
		}, {
			name: "invalid id",
			opts: []Option{
				ID("123!"),
				Reader(bytes.NewReader([]byte("HelloWorld"))),
			},
			expected: nil,
			err:      ErrInvalidInput,
		}, {
			name: "missing id",
			opts: []Option{
				Reader(bytes.NewReader([]byte("HelloWorld"))),
			},
			expected: nil,
			err:      ErrInvalidInput,
		}, {
			name: "nil stream",
			opts: []Option{
				ID("123"),
				EstimatedLength(10),
			},
			expected: nil,
			err:      ErrInvalidInput,
		}, {
			name: "invalid encoding",
			opts: []Option{
				ID("123"),
				Reader(bytes.NewReader([]byte("HelloWorld"))),
				WithEncoding("invalid"),
			},
			expected: nil,
			err:      ErrInvalidInput,
		}, {
			name: "default max packet size",
			opts: []Option{
				ID("123"),
				Reader(bytes.NewReader([]byte("HelloWorld"))),
				EstimatedLength(10),
				MaxPacketSize(0),
			},
			expected: &Packetizer{
				id:            "123",
				estimatedSize: 10,
				maxPacketSize: 64 * 1024,
			},
			err: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := New(tt.opts...)
			if tt.err != nil {
				assert.True(t, errors.Is(err, tt.err))
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected.id, p.id)
				assert.Equal(t, tt.expected.estimatedSize, p.estimatedSize)
				assert.Equal(t, tt.expected.maxPacketSize, p.maxPacketSize)
				assert.NotNil(t, p.stream)
			}
		})
	}
}

func TestPacketizer_Next(t *testing.T) {
	errUnknown := errors.New("unknown error")

	tests := []struct {
		name       string
		opts       []Option
		in         wrp.Message
		expected   []wrp.Message
		packetizer *Packetizer // not used except for very specific tests
		noVadors   bool
		extra      bool
		err        error
	}{
		{
			name: "valid next packet",
			opts: []Option{
				ID("123"),
				Reader(bytes.NewReader([]byte("HelloWorld"))),
				EstimatedLength(10),
				MaxPacketSize(5),
				WithEncoding(EncodingIdentity),
			},
			in: wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "mac:112233445566",
				Destination: "event:device-status",
			},
			expected: []wrp.Message{
				{
					Type:        wrp.SimpleEventMessageType,
					Source:      "mac:112233445566",
					Destination: "event:device-status",
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 0",
						"stream-estimated-total-length: 10",
					},
					Payload: []byte("Hello"),
				},
				{
					Type:        wrp.SimpleEventMessageType,
					Source:      "mac:112233445566",
					Destination: "event:device-status",
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 1",
						"stream-estimated-total-length: 10",
					},
					Payload: []byte("World"),
				},
				{
					Type:        wrp.SimpleEventMessageType,
					Source:      "mac:112233445566",
					Destination: "event:device-status",
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 2",
						"stream-estimated-total-length: 10",
						"stream-final-packet: eof",
					},
				},
			},
			err: io.EOF,
		}, {
			name: "valid next packet, request response",
			opts: []Option{
				ID("123"),
				Reader(bytes.NewReader([]byte("HelloWorld"))),
				EstimatedLength(10),
				MaxPacketSize(5),
				WithEncoding(EncodingIdentity),
				WithUpdateTransactionUUID(func() (string, error) {
					return "ABC", nil
				}),
			},
			in: wrp.Message{
				Type:        wrp.SimpleRequestResponseMessageType,
				Source:      "mac:112233445566",
				Destination: "event:device-status",
				// TransactionUUID: not needed, will be set by the packetizer
			},
			expected: []wrp.Message{
				{
					Type:            wrp.SimpleRequestResponseMessageType,
					Source:          "mac:112233445566",
					Destination:     "event:device-status",
					TransactionUUID: "ABC",
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 0",
						"stream-estimated-total-length: 10",
					},
					Payload: []byte("Hello"),
				},
				{
					Type:            wrp.SimpleRequestResponseMessageType,
					Source:          "mac:112233445566",
					Destination:     "event:device-status",
					TransactionUUID: "ABC",
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 1",
						"stream-estimated-total-length: 10",
					},
					Payload: []byte("World"),
				},
				{
					Type:            wrp.SimpleRequestResponseMessageType,
					Source:          "mac:112233445566",
					Destination:     "event:device-status",
					TransactionUUID: "ABC",
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 2",
						"stream-estimated-total-length: 10",
						"stream-final-packet: eof",
					},
				},
			},
			err: io.EOF,
		}, {
			name: "valid next packet, streaming",
			opts: []Option{
				ID("123"),
				Reader(bytes.NewReader([]byte("HelloWorld"))),
				MaxPacketSize(5),
				WithEncoding(EncodingIdentity),
			},
			in: wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "mac:112233445566",
				Destination: "event:device-status",
			},
			expected: []wrp.Message{
				{
					Type:        wrp.SimpleEventMessageType,
					Source:      "mac:112233445566",
					Destination: "event:device-status",
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 0",
					},
					Payload: []byte("Hello"),
				},
				{
					Type:        wrp.SimpleEventMessageType,
					Source:      "mac:112233445566",
					Destination: "event:device-status",
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 1",
					},
					Payload: []byte("World"),
				},
				{
					Type:        wrp.SimpleEventMessageType,
					Source:      "mac:112233445566",
					Destination: "event:device-status",
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 2",
						"stream-final-packet: eof",
					},
				},
			},
			err: io.EOF,
		}, {
			name:     "valid next packet, streaming, no validation",
			noVadors: true,
			opts: []Option{
				ID("123"),
				Reader(bytes.NewReader([]byte("HelloWorld"))),
				MaxPacketSize(5),
				WithEncoding(EncodingIdentity),
			},
			in: wrp.Message{
				Type:        wrp.SimpleRequestResponseMessageType,
				Source:      "mac:112233445566",
				Destination: "event:device-status",
			},
			expected: []wrp.Message{
				{
					Type:        wrp.SimpleRequestResponseMessageType,
					Source:      "mac:112233445566",
					Destination: "event:device-status",
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 0",
					},
					Payload: []byte("Hello"),
				},
				{
					Type:        wrp.SimpleRequestResponseMessageType,
					Source:      "mac:112233445566",
					Destination: "event:device-status",
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 1",
					},
					Payload: []byte("World"),
				},
				{
					Type:        wrp.SimpleRequestResponseMessageType,
					Source:      "mac:112233445566",
					Destination: "event:device-status",
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 2",
						"stream-final-packet: eof",
					},
				},
			},
			err: io.EOF,
		}, {
			name: "reader is shorter than told",
			opts: []Option{
				ID("123"),
				Reader(bytes.NewReader([]byte("HelloWorld"))),
				EstimatedLength(20),
				MaxPacketSize(6),
				WithEncoding(EncodingIdentity),
			},
			in: wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "mac:112233445566",
				Destination: "event:device-status",
			},
			expected: []wrp.Message{
				{
					Type:        wrp.SimpleEventMessageType,
					Source:      "mac:112233445566",
					Destination: "event:device-status",
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 0",
						"stream-estimated-total-length: 20",
					},
					Payload: []byte("HelloW"),
				},
				{
					Type:        wrp.SimpleEventMessageType,
					Source:      "mac:112233445566",
					Destination: "event:device-status",
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 1",
						"stream-estimated-total-length: 20",
						"stream-final-packet: eof",
					},
					Payload: []byte("orld"),
				},
			},
			err: io.EOF,
		}, {
			name: "faulty reader",
			opts: []Option{
				ID("123"),
				Reader(
					&faultyReader{
						Reader: bytes.NewReader([]byte("HelloWorld")),
						when:   7,
					}),
				EstimatedLength(20),
				MaxPacketSize(6),
				WithEncoding(EncodingIdentity),
			},
			in: wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "mac:112233445566",
				Destination: "event:device-status",
			},
			expected: []wrp.Message{
				{
					Type: wrp.SimpleEventMessageType,
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 0",
						"stream-estimated-total-length: 20",
					},
					Payload: []byte("HelloW"),
				}, {
					Type: wrp.SimpleEventMessageType,
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 1",
						"stream-estimated-total-length: 20",
						"stream-final-packet: unexpected EOF",
					},
					Payload: []byte("o"),
				},
			},
			err:   io.ErrUnexpectedEOF,
			extra: true,
		},
		{
			name: "invalid wrp.Message",
			opts: []Option{
				ID("123"),
				Reader(
					&faultyReader{
						Reader: bytes.NewReader([]byte("HelloWorld")),
						when:   7,
					}),
				EstimatedLength(20),
				MaxPacketSize(6),
				WithEncoding(EncodingIdentity),
			},
			in: wrp.Message{
				Type:   wrp.SimpleEventMessageType,
				Source: "mac:112233445566",
				// Missing Destination
			},
			err:   wrp.ErrMessageIsInvalid,
			extra: true,
		}, {
			name: "encoding error falls back to identity encoding",
			packetizer: func() *Packetizer {
				p, err := New(
					ID("123"),
					Reader(bytes.NewReader([]byte("HelloWorld"))))
				require.NoError(t, err)
				require.NotNil(t, p)
				// Set invalid encoding to trigger fallback in compress()
				p.encoding = "invalid"
				return p
			}(),
			in: wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "mac:112233445566",
				Destination: "event:device-status",
			},
			expected: []wrp.Message{
				{
					Type: wrp.SimpleEventMessageType,
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 0",
						"stream-final-packet: eof",
						// Note: No stream-encoding header means identity encoding (fallback)
					},
					Payload: []byte("HelloWorld"),
				},
			},
			err: io.EOF, // Stream completes successfully despite encoding error
		}, {
			name: "the transaction uuid function returns an error",
			opts: []Option{
				ID("123"),
				Reader(bytes.NewReader([]byte("HelloWorld"))),
				EstimatedLength(10),
				MaxPacketSize(5),
				WithEncoding(EncodingIdentity),
				WithUpdateTransactionUUID(func() (string, error) {
					return "", errUnknown
				}),
			},
			in: wrp.Message{
				Type:            wrp.SimpleRequestResponseMessageType,
				Source:          "mac:112233445566",
				Destination:     "event:device-status",
				TransactionUUID: "1234567890",
			},
			err:   errUnknown,
			extra: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if errors.Is(tt.err, context.Canceled) {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			var packetizer *Packetizer

			if tt.packetizer != nil {
				packetizer = tt.packetizer
			} else {
				var err error
				packetizer, err = New(tt.opts...)
				require.NoError(t, err)
				require.NotNil(t, packetizer)
			}

			for i, expected := range tt.expected {
				var vadors []wrp.Processor
				if tt.noVadors {
					vadors = []wrp.Processor{
						wrp.NoStandardValidation(),
					}
				}
				got, err := packetizer.Next(ctx, tt.in, vadors...)

				assert.NotEmpty(t, got, "message %d should not be nil", i)
				require.NotNil(t, got, "message %d should not be nil", i)
				assert.Equal(t, expected.Headers, got.Headers, "message %d headers", i)
				if expected.Payload == nil {
					assert.Empty(t, got.Payload, "message %d payload should be empty", i)
				} else {
					assert.Equal(t, expected.Payload, got.Payload, "message %d payload", i)
				}

				if i < len(tt.expected)-1 {
					assert.NoError(t, err, "message %d should not error", i)
					continue
				}

				if tt.err == nil {
					assert.NoError(t, err, "message %d should have no error", i)
				} else {
					assert.Error(t, err, "message %d should have error", i)
					if !errors.Is(tt.err, errUnknown) {
						assert.ErrorIs(t, err, tt.err, "message %d should have correct error", i)
					}
				}
			}

			if tt.extra {
				got, err := packetizer.Next(ctx, tt.in)
				assert.Empty(t, got)
				assert.Error(t, err)
				if !errors.Is(tt.err, errUnknown) {
					assert.ErrorIs(t, err, tt.err)
				}
			}
		})
	}
}

func TestPacketizer_FrameBoundaryAlignment(t *testing.T) {
	tests := []struct {
		name          string
		input         []byte
		maxPacketSize int
	}{
		{
			name:          "exact multiple of packet size",
			input:         []byte("HelloWorld"), // 10 bytes, packet size 5
			maxPacketSize: 5,
		},
		{
			name:          "not a multiple of packet size",
			input:         []byte("HelloWorld!"), // 11 bytes, packet size 5
			maxPacketSize: 5,
		},
		{
			name:          "single byte packets",
			input:         []byte("ABCDEF"),
			maxPacketSize: 1,
		},
		{
			name:          "packet size larger than input",
			input:         []byte("Hi"),
			maxPacketSize: 100,
		},
		{
			name:          "single packet exactly",
			input:         []byte("12345"),
			maxPacketSize: 5,
		},
		{
			name:          "odd sizes",
			input:         []byte("ABCDEFGHIJKLMNOP"), // 16 bytes
			maxPacketSize: 7,                          // 7 + 7 + 2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packetizer, err := New(
				ID("test"),
				Reader(bytes.NewReader(tt.input)),
				MaxPacketSize(tt.maxPacketSize),
				WithEncoding(EncodingIdentity),
			)
			require.NoError(t, err)

			in := wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "mac:112233445566",
				Destination: "event:device-status",
			}

			// Collect all payloads
			var payloads [][]byte
			var expectedPacketNumber int64
			for {
				got, err := packetizer.Next(context.Background(), in)

				if got != nil {
					// Verify sequential packet numbers
					foundPacketNumber := false
					for _, h := range got.Headers {
						if len(h) > 22 && h[:22] == "stream-packet-number: " {
							numberStr := h[22:]
							actualNumber, parseErr := strconv.ParseInt(numberStr, 10, 64)
							require.NoError(t, parseErr, "failed to parse packet number from header: %s", h)
							assert.Equal(t, expectedPacketNumber, actualNumber,
								"packet number should be sequential")
							foundPacketNumber = true
							break
						}
					}
					assert.True(t, foundPacketNumber, "packet should have stream-packet-number header")

					if len(got.Payload) > 0 {
						payloads = append(payloads, got.Payload)
					}
					expectedPacketNumber++
				}

				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}

			// Reassemble and verify
			reassembled := bytes.Join(payloads, nil)
			assert.Equal(t, tt.input, reassembled,
				"reassembled data should match original input")

			// Verify each payload (except last) is exactly maxPacketSize
			for i, payload := range payloads[:len(payloads)-1] {
				assert.Equal(t, tt.maxPacketSize, len(payload),
					"packet %d should be exactly maxPacketSize", i)
			}
		})
	}
}

func TestPacketizer_FrameBoundaryAfterContextCancel(t *testing.T) {
	// Verify that context cancellation doesn't corrupt frame boundaries
	input := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

	packetizer, err := New(
		ID("boundary-test"),
		Reader(bytes.NewReader(input)),
		MaxPacketSize(5),
		WithEncoding(EncodingIdentity),
	)
	require.NoError(t, err)

	in := wrp.Message{
		Type:        wrp.SimpleEventMessageType,
		Source:      "mac:112233445566",
		Destination: "event:device-status",
	}

	// Cancel context a few times before reading
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		got, err := packetizer.Next(ctx, in)
		assert.Nil(t, got)
		assert.ErrorIs(t, err, context.Canceled)
	}

	// Now read all packets and verify alignment
	var payloads [][]byte
	for {
		got, err := packetizer.Next(context.Background(), in)
		if got != nil && len(got.Payload) > 0 {
			payloads = append(payloads, got.Payload)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}

	reassembled := bytes.Join(payloads, nil)
	assert.Equal(t, input, reassembled,
		"data should be intact after context cancellations")

	// Verify packet boundaries: ABCDE, FGHIJ, KLMNO, PQRST, UVWXY, Z
	expected := []string{"ABCDE", "FGHIJ", "KLMNO", "PQRST", "UVWXY", "Z"}
	require.Equal(t, len(expected), len(payloads))
	for i, exp := range expected {
		assert.Equal(t, exp, string(payloads[i]), "packet %d content", i)
	}
}

type faultyReader struct {
	io.Reader
	when    int
	current int
}

func (f *faultyReader) Read(p []byte) (int, error) {
	n, err := f.Reader.Read(p)
	prev := f.current
	f.current += n
	if f.current >= f.when {
		err = io.ErrUnexpectedEOF
		n = f.when - prev
	}

	return n, err
}

func TestPacketizer_ContextCancellation(t *testing.T) {
	t.Run("data available after context cancel is returned", func(t *testing.T) {
		// This test verifies that if data arrives BEFORE context cancellation
		// is detected, the data is returned (not the cancellation error).
		// This uses a regular reader - no blocking.

		packetizer, err := New(
			ID("789"),
			Reader(bytes.NewReader([]byte("HelloWorld"))),
			MaxPacketSize(5),
			WithEncoding(EncodingIdentity),
		)
		require.NoError(t, err)

		in := wrp.Message{
			Type:        wrp.SimpleEventMessageType,
			Source:      "mac:112233445566",
			Destination: "event:device-status",
		}

		// Read first packet successfully
		got, err := packetizer.Next(context.Background(), in)
		assert.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, []byte("Hello"), got.Payload)

		// Read second packet successfully
		got, err = packetizer.Next(context.Background(), in)
		assert.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, []byte("World"), got.Payload)
	})

	t.Run("canceled context returns nil without affecting stream", func(t *testing.T) {
		packetizer, err := New(
			ID("123"),
			Reader(bytes.NewReader([]byte("HelloWorld"))),
			MaxPacketSize(5),
			WithEncoding(EncodingIdentity),
		)
		require.NoError(t, err)

		in := wrp.Message{
			Type:        wrp.SimpleEventMessageType,
			Source:      "mac:112233445566",
			Destination: "event:device-status",
		}

		// Cancel context before calling Next
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// Should return nil, context.Canceled
		got, err := packetizer.Next(ctx, in)
		assert.Nil(t, got)
		assert.ErrorIs(t, err, context.Canceled)

		// Stream should still be usable - call Next with valid context
		got, err = packetizer.Next(context.Background(), in)
		assert.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, []byte("Hello"), got.Payload)
		assert.Equal(t, []string{
			"stream-id: 123",
			"stream-packet-number: 0",
		}, got.Headers)

		// Continue reading the rest
		got, err = packetizer.Next(context.Background(), in)
		assert.NoError(t, err)
		assert.NotNil(t, got)
		assert.Equal(t, []byte("World"), got.Payload)

		// Final packet
		got, err = packetizer.Next(context.Background(), in)
		assert.ErrorIs(t, err, io.EOF)
		assert.NotNil(t, got)
		assert.Contains(t, got.Headers, "stream-final-packet: eof")
	})

	t.Run("multiple canceled contexts do not affect stream", func(t *testing.T) {
		packetizer, err := New(
			ID("456"),
			Reader(bytes.NewReader([]byte("TestData"))),
			MaxPacketSize(4),
			WithEncoding(EncodingIdentity),
		)
		require.NoError(t, err)

		in := wrp.Message{
			Type:        wrp.SimpleEventMessageType,
			Source:      "mac:112233445566",
			Destination: "event:device-status",
		}

		// Cancel multiple times
		for i := 0; i < 3; i++ {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			got, err := packetizer.Next(ctx, in)
			assert.Nil(t, got)
			assert.ErrorIs(t, err, context.Canceled)
		}

		// Stream should still work
		got, err := packetizer.Next(context.Background(), in)
		assert.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, []byte("Test"), got.Payload)

		// Packet number should still be 0 since no successful packets were returned
		assert.Contains(t, got.Headers, "stream-packet-number: 0")
	})
}

// noProgressReader simulates a buggy reader that returns (0, nil) without making progress
type noProgressReader struct {
	mu        sync.Mutex
	callCount int
}

func (r *noProgressReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	r.callCount++
	r.mu.Unlock()
	// Always return (0, nil) - a buggy reader that makes no progress
	return 0, nil
}

func TestPacketizer_NoProgressReader(t *testing.T) {
	reader := &noProgressReader{}
	packetizer, err := New(
		ID("test"),
		Reader(reader),
		MaxPacketSize(10),
		WithEncoding(EncodingIdentity),
	)
	require.NoError(t, err)

	in := wrp.Message{
		Type:        wrp.SimpleEventMessageType,
		Source:      "mac:112233445566",
		Destination: "event:device-status",
	}

	got, err := packetizer.Next(context.Background(), in)

	// Should return io.ErrNoProgress immediately on first (0, nil) response
	assert.ErrorIs(t, err, io.ErrNoProgress, "should detect no-progress reader")
	require.NotNil(t, got, "should return partial message even on error")
	assert.Empty(t, got.Payload, "payload should be empty since no data was read")

	// Verify it only called Read once (didn't loop forever)
	reader.mu.Lock()
	count := reader.callCount
	reader.mu.Unlock()
	assert.Equal(t, 1, count, "should only call Read once before detecting no progress")
}
