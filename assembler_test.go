// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xmidt-org/wrp-go/v3"
)

func TestAssembler_Read(t *testing.T) {
	tests := []struct {
		name     string
		blocks   map[uint64]block
		bufSize  int
		expected []string
		finalErr error
	}{
		{
			name: "multiple blocks",
			blocks: map[uint64]block{
				0: {headers: headers{}, payload: []byte("Hello, ")},
				1: {headers: headers{finalPacket: "EOF"}, payload: []byte("World!")},
			},
			bufSize:  7,
			expected: []string{"Hello, ", "World!"},
			finalErr: io.EOF,
		}, {
			name: "incomplete block",
			blocks: map[uint64]block{
				0: {headers: headers{finalPacket: "EOF"}, payload: []byte("Hello")},
			},
			bufSize:  10,
			expected: []string{"Hello"},
			finalErr: io.EOF,
		}, {
			name: "custom error",
			blocks: map[uint64]block{
				0: {headers: headers{finalPacket: "Oops"}, payload: []byte("Hello")},
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
				assembler.blocks = tt.blocks
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
		blocks: map[uint64]block{
			1: {headers: headers{finalPacket: "EOF"}, payload: []byte("Hello")},
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
		expected  map[uint64]block
		err       error
	}{
		{
			name:      "valid message",
			assembler: &Assembler{},
			msg: wrp.Message{
				Type: wrp.SimpleEventMessageType,
				Headers: []string{"stream-id: 1",
					"stream-packet-number: 0",
					"stream-estimated-total-length: 5",
				},
				Payload: []byte("Hello"),
			},
			expected: map[uint64]block{
				0: {
					headers: headers{
						id:                  "1",
						currentPacketNumber: 0,
						totalLength:         5,
					},
					payload: []byte("Hello"),
				},
			},
			err: nil,
		}, {
			name:      "valid message",
			assembler: &Assembler{},
			msg: wrp.Message{
				Type: wrp.SimpleEventMessageType,
				Headers: []string{"stream-id: 1",
					"stream-packet-number: 0",
					"stream-estimated-total-length: 0",
				},
				Payload: []byte("Hello"),
			},
			expected: map[uint64]block{
				0: {
					headers: headers{
						id:                  "1",
						currentPacketNumber: 0,
					},
					payload: []byte("Hello"),
				},
			},
			err: nil,
		}, {
			name: "invalid message type",
			assembler: &Assembler{
				blocks: make(map[uint64]block),
			},
			msg: wrp.Message{
				Type:    wrp.SimpleRequestResponseMessageType,
				Headers: []string{"stream-id: 1", "stream-packet-number: 0"},
				Payload: []byte("Hello"),
			},
			expected: map[uint64]block{},
			err:      errNotHandled,
		}, {
			name: "no message number",
			assembler: &Assembler{
				blocks: make(map[uint64]block),
			},
			msg: wrp.Message{
				Type:    wrp.SimpleEventMessageType,
				Headers: []string{"stream-id: 1"},
				Payload: []byte("Hello"),
			},
			expected: map[uint64]block{},
			err:      ErrInvalidInput,
		}, {
			name: "closed assembler",
			assembler: &Assembler{
				closed: true,
				blocks: make(map[uint64]block),
			},
			msg: wrp.Message{
				Type:    wrp.SimpleEventMessageType,
				Headers: []string{"stream-id: 1", "stream-packet-number: 0"},
				Payload: []byte("Hello"),
			},
			expected: map[uint64]block{},
			err:      ErrClosed,
		}, {
			name: "duplicate packet",
			assembler: &Assembler{
				blocks: map[uint64]block{
					0: {
						headers: headers{
							id:                  "1",
							currentPacketNumber: 0,
						},
						payload: []byte("Hello"),
					},
				},
			},
			msg: wrp.Message{
				Type:    wrp.SimpleEventMessageType,
				Headers: []string{"stream-id: 1", "stream-packet-number: 0"},
				Payload: []byte("World"),
			},
			expected: map[uint64]block{
				0: {
					headers: headers{
						id:                  "1",
						currentPacketNumber: 0,
					},
					payload: []byte("Hello"),
				},
			},
			err: nil,
		}, {
			name: "late duplicate packet",
			assembler: &Assembler{
				current: 4,
				blocks: map[uint64]block{
					4: {
						headers: headers{
							id:                  "1",
							currentPacketNumber: 4,
						},
						payload: []byte("Hello"),
					},
				},
			},
			msg: wrp.Message{
				Type:    wrp.SimpleEventMessageType,
				Headers: []string{"stream-id: 1", "stream-packet-number: 1"},
				Payload: []byte("World"),
			},
			expected: map[uint64]block{
				4: {
					headers: headers{
						id:                  "1",
						currentPacketNumber: 4,
					},
					payload: []byte("Hello"),
				},
			},
			err: nil,
		}, {
			name: "invalid packet",
			assembler: &Assembler{
				blocks: map[uint64]block{
					0: {
						headers: headers{
							id:                  "1",
							currentPacketNumber: 0,
						},
						payload: []byte("Hello"),
					},
				},
			},
			msg: wrp.Message{
				Type:    wrp.SimpleEventMessageType,
				Headers: []string{"stream-id: 1", "stream-packet-number: -1"},
				Payload: []byte("World"),
			},
			expected: map[uint64]block{
				0: {
					headers: headers{
						id:                  "1",
						currentPacketNumber: 0,
					},
					payload: []byte("Hello"),
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
			assert.Equal(t, tt.expected, tt.assembler.blocks)
		})
	}
}
