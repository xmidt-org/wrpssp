// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xmidt-org/wrp-go/v5"
)

func TestAssembler_Read(t *testing.T) {
	tests := []struct {
		name     string
		blocks   map[int64]*simpleStreamingMessage
		bufSize  int
		expected []string
		finalErr error
	}{
		{
			name: "multiple blocks",
			blocks: map[int64]*simpleStreamingMessage{
				0: {
					SimpleEvent: wrp.SimpleEvent{
						Payload: []byte("Hello, "),
					},
				},
				1: {
					StreamFinalPacket: "EOF",
					SimpleEvent: wrp.SimpleEvent{
						Payload: []byte("World!"),
					},
				},
			},
			bufSize:  7,
			expected: []string{"Hello, ", "World!"},
			finalErr: io.EOF,
		}, {
			name: "incomplete block",
			blocks: map[int64]*simpleStreamingMessage{
				0: {
					StreamFinalPacket: "EOF",
					SimpleEvent: wrp.SimpleEvent{
						Payload: []byte("Hello"),
					},
				},
			},
			bufSize:  10,
			expected: []string{"Hello"},
			finalErr: io.EOF,
		}, {
			name: "custom error",
			blocks: map[int64]*simpleStreamingMessage{
				0: {
					StreamFinalPacket: "Oops",
					SimpleEvent: wrp.SimpleEvent{
						Payload: []byte("Hello"),
					},
				},
			},
			bufSize:  10,
			expected: []string{"Hello"},
			finalErr: io.ErrUnexpectedEOF,
		}, {
			name:     "empty blocks",
			bufSize:  10,
			expected: []string{""},
			finalErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assembler := Assembler{}
			if tt.blocks != nil {
				assembler.packets = tt.blocks
			}

			buf := make([]byte, tt.bufSize)
			for i, exp := range tt.expected {
				n, err := assembler.Read(buf)
				assert.Equal(t, len(exp), n)
				assert.Equal(t, exp, string(buf[:n]))
				if i < len(tt.expected)-1 {
					assert.NoError(t, err)
				} else {
					if tt.finalErr != nil {
						assert.ErrorIs(t, err, tt.finalErr)
					} else {
						assert.NoError(t, err)
					}
				}
			}

			n, err := assembler.Read(buf)
			assert.ErrorIs(t, err, tt.finalErr)
			assert.Equal(t, 0, n)
		})
	}
}

func TestAssembler_Close(t *testing.T) {
	assembler := &Assembler{
		packets: map[int64]*simpleStreamingMessage{
			1: {
				StreamFinalPacket: "EOF",
				SimpleEvent: wrp.SimpleEvent{
					Payload: []byte("Hello"),
				},
			},
		},
	}

	err := assembler.Close()
	assert.NoError(t, err)
	assert.True(t, assembler.closed)
}

func TestAssembler_ProcessWRP(t *testing.T) {
	tests := []struct {
		name      string
		assembler *Assembler
		msg       wrp.Message
		expected  map[int64]*simpleStreamingMessage
		err       error
	}{
		{
			name:      "valid message",
			assembler: &Assembler{},
			msg: wrp.Message{
				Source:      "mac:112233445566",
				Destination: "event:status/mac:112233445566",
				Type:        wrp.SimpleEventMessageType,
				Headers: []string{
					"stream-id: 1",
					"stream-packet-number: 0",
					"stream-estimated-total-length: 5",
				},
				Payload: []byte("Hello"),
			},
			expected: map[int64]*simpleStreamingMessage{
				0: {
					StreamID:              "1",
					StreamPacketNumber:    0,
					StreamEstimatedLength: 5,
					SimpleEvent: wrp.SimpleEvent{
						Source:      "mac:112233445566",
						Destination: "event:status/mac:112233445566",
						Payload:     []byte("Hello"),
					},
				},
			},
			err: nil,
		}, {
			name:      "valid message, 0 estimated length",
			assembler: &Assembler{},
			msg: wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "mac:112233445566",
				Destination: "event:status/mac:112233445566",
				Headers: []string{"stream-id: 1",
					"stream-packet-number: 0",
					"stream-estimated-total-length: 0",
				},
				Payload: []byte("Hello"),
			},
			expected: map[int64]*simpleStreamingMessage{
				0: {
					StreamID:           "1",
					StreamPacketNumber: 0,
					SimpleEvent: wrp.SimpleEvent{
						Source:      "mac:112233445566",
						Destination: "event:status/mac:112233445566",
						Payload:     []byte("Hello"),
					},
				},
			},
			err: nil,
		}, {
			name: "invalid message type",
			assembler: &Assembler{
				packets: make(map[int64]*simpleStreamingMessage),
			},
			msg: wrp.Message{
				Type:        wrp.SimpleRequestResponseMessageType,
				Source:      "mac:112233445566",
				Destination: "event:status/mac:112233445566",
				Headers:     []string{"stream-id: 1", "stream-packet-number: 0"},
				Payload:     []byte("Hello"),
			},
			expected: map[int64]*simpleStreamingMessage{},
			err:      wrp.ErrNotHandled,
		}, {
			name: "no message number",
			assembler: &Assembler{
				packets: make(map[int64]*simpleStreamingMessage),
			},
			msg: wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "mac:112233445566",
				Destination: "event:status/mac:112233445566",
				Headers:     []string{"stream-id: 1"},
				Payload:     []byte("Hello"),
			},
			expected: map[int64]*simpleStreamingMessage{},
			err:      ErrInvalidInput,
		}, {
			name: "closed assembler",
			assembler: &Assembler{
				closed:  true,
				packets: make(map[int64]*simpleStreamingMessage),
			},
			msg: wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "mac:112233445566",
				Destination: "event:status/mac:112233445566",
				Headers:     []string{"stream-id: 1", "stream-packet-number: 0"},
				Payload:     []byte("Hello"),
			},
			expected: map[int64]*simpleStreamingMessage{},
			err:      ErrClosed,
		}, {
			name: "duplicate packet",
			assembler: &Assembler{
				packets: map[int64]*simpleStreamingMessage{
					0: {
						StreamID:           "1",
						StreamPacketNumber: 0,
						SimpleEvent: wrp.SimpleEvent{
							Source:      "mac:112233445566",
							Destination: "event:status/mac:112233445566",
							Payload:     []byte("Hello"),
						},
					},
				},
			},
			msg: wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "mac:112233445566",
				Destination: "event:status/mac:112233445566",
				Headers:     []string{"stream-id: 1", "stream-packet-number: 0"},
				Payload:     []byte("World"),
			},
			expected: map[int64]*simpleStreamingMessage{
				0: {
					StreamID:           "1",
					StreamPacketNumber: 0,
					SimpleEvent: wrp.SimpleEvent{
						Source:      "mac:112233445566",
						Destination: "event:status/mac:112233445566",
						Payload:     []byte("Hello"),
					},
				},
			},
			err: nil,
		}, {
			name: "late duplicate packet",
			assembler: &Assembler{
				current: 4,
				packets: map[int64]*simpleStreamingMessage{
					4: {
						StreamID:           "1",
						StreamPacketNumber: 4,
						SimpleEvent: wrp.SimpleEvent{
							Source:      "mac:112233445566",
							Destination: "event:status/mac:112233445566",
							Payload:     []byte("Hello"),
						},
					},
				},
			},
			msg: wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "mac:112233445566",
				Destination: "event:status/mac:112233445566",
				Headers:     []string{"stream-id: 1", "stream-packet-number: 1"},
				Payload:     []byte("World"),
			},
			expected: map[int64]*simpleStreamingMessage{
				4: {
					StreamID:           "1",
					StreamPacketNumber: 4,
					SimpleEvent: wrp.SimpleEvent{
						Source:      "mac:112233445566",
						Destination: "event:status/mac:112233445566",
						Payload:     []byte("Hello"),
					},
				},
			},
			err: nil,
		}, {
			name: "invalid packet",
			assembler: &Assembler{
				packets: map[int64]*simpleStreamingMessage{
					0: {
						StreamID:           "1",
						StreamPacketNumber: 0,
						SimpleEvent: wrp.SimpleEvent{
							Source:      "mac:112233445566",
							Destination: "event:status/mac:112233445566",
							Payload:     []byte("Hello"),
						},
					},
				},
			},
			msg: wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "mac:112233445566",
				Destination: "event:status/mac:112233445566",
				Headers:     []string{"stream-id: 1", "stream-packet-number: -1"},
				Payload:     []byte("World"),
			},
			expected: map[int64]*simpleStreamingMessage{
				0: {
					StreamID:           "1",
					StreamPacketNumber: 0,
					SimpleEvent: wrp.SimpleEvent{
						Source:      "mac:112233445566",
						Destination: "event:status/mac:112233445566",
						Payload:     []byte("Hello"),
					},
				},
			},
			err: ErrInvalidInput,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			err := tt.assembler.ProcessWRP(ctx, tt.msg)
			assert.ErrorIs(t, err, tt.err)
			assert.Equal(t, tt.expected, tt.assembler.packets)
		})
	}
}
