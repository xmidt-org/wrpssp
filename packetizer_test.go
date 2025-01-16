// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xmidt-org/wrp-go/v3"
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
				ReaderLength(10),
				MaxPacketSize(5),
			},
			expected: &Packetizer{
				headers: headers{
					id:          "123",
					totalLength: 10,
				},
				stream:        bytes.NewReader([]byte("HelloWorld")),
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
				ReaderLength(10),
			},
			expected: nil,
			err:      ErrInvalidInput,
		}, {
			name: "default max packet size",
			opts: []Option{
				ID("123"),
				Reader(bytes.NewReader([]byte("HelloWorld"))),
				ReaderLength(10),
				MaxPacketSize(0),
			},
			expected: &Packetizer{
				headers: headers{
					id:          "123",
					totalLength: 10,
				},
				stream:        bytes.NewReader([]byte("HelloWorld")),
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
				assert.Equal(t, tt.expected.headers.id, p.headers.id)
				assert.Equal(t, tt.expected.headers.totalLength, p.headers.totalLength)
				assert.Equal(t, tt.expected.maxPacketSize, p.maxPacketSize)
				assert.NotNil(t, p.stream)
			}
		})
	}
}

func TestPacketizer_Next(t *testing.T) {
	tests := []struct {
		name     string
		opts     []Option
		expected []wrp.Message
		extra    bool
		err      error
	}{
		{
			name: "valid next packet",
			opts: []Option{
				ID("123"),
				Reader(bytes.NewReader([]byte("HelloWorld"))),
				ReaderLength(10),
				MaxPacketSize(5),
			},
			expected: []wrp.Message{
				{
					Type: wrp.SimpleEventMessageType,
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 0",
						"stream-estimated-total-length: 10",
					},
					Payload: []byte("Hello"),
				},
				{
					Type: wrp.SimpleEventMessageType,
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 1",
						"stream-estimated-total-length: 10",
					},
					Payload: []byte("World"),
				},
				{
					Type: wrp.SimpleEventMessageType,
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 2",
						"stream-estimated-total-length: 10",
						"stream-final-packet: EOF",
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
			},
			expected: []wrp.Message{
				{
					Type: wrp.SimpleEventMessageType,
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 0",
					},
					Payload: []byte("Hello"),
				},
				{
					Type: wrp.SimpleEventMessageType,
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 1",
					},
					Payload: []byte("World"),
				},
				{
					Type: wrp.SimpleEventMessageType,
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 2",
						"stream-final-packet: EOF",
					},
				},
			},
			err: io.EOF,
		}, {
			name: "reader is shorter than told",
			opts: []Option{
				ID("123"),
				Reader(bytes.NewReader([]byte("HelloWorld"))),
				ReaderLength(20),
				MaxPacketSize(6),
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
				},
				{
					Type: wrp.SimpleEventMessageType,
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 1",
						"stream-estimated-total-length: 20",
					},
					Payload: []byte("orld"),
				},
				{
					Type: wrp.SimpleEventMessageType,
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 2",
						"stream-estimated-total-length: 20",
						"stream-final-packet: EOF",
					},
				},
			},
			err: io.EOF,
		}, {
			name: "context cancelled",
			opts: []Option{
				ID("123"),
				Reader(bytes.NewReader([]byte("HelloWorld"))),
				ReaderLength(20),
				MaxPacketSize(6),
			},
			expected: []wrp.Message{
				{
					Type: wrp.SimpleEventMessageType,
					Headers: []string{
						"stream-id: 123",
						"stream-packet-number: 0",
						"stream-estimated-total-length: 20",
						"stream-final-packet: context canceled",
					},
				},
			},
			err: context.Canceled,
		}, {
			name: "faulty reader",
			opts: []Option{
				ID("123"),
				Reader(
					&faultyReader{
						Reader: bytes.NewReader([]byte("HelloWorld")),
						when:   7,
					}),
				ReaderLength(20),
				MaxPacketSize(6),
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.err == context.Canceled {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			packetizer, err := New(tt.opts...)
			require.Nil(t, err)
			require.NotNil(t, packetizer)

			for i, expected := range tt.expected {
				got, err := packetizer.Next(ctx)

				assert.NotEmpty(t, got)
				assert.Equal(t, expected.Type, got.Type)
				assert.Equal(t, expected.Headers, got.Headers)
				assert.Equal(t, expected.Payload, got.Payload)

				if i < len(tt.expected)-1 {
					assert.Nil(t, err)
					continue
				}
				assert.ErrorIs(t, err, tt.err)
			}

			if tt.extra {
				got, err := packetizer.Next(ctx)
				assert.Empty(t, got)
				assert.ErrorIs(t, err, tt.err)
			}
		})
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
