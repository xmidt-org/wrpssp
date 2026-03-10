// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xmidt-org/wrp-go/v5"
)

func TestAssembler_Read(t *testing.T) {
	t.Parallel()

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
					Message: wrp.Message{
						Payload: []byte("Hello, "),
					},
				},
				1: {
					StreamFinalPacket: "EOF",
					Message: wrp.Message{
						Payload: []byte("World!"),
					},
				},
			},
			bufSize:  7,
			expected: []string{"Hello, ", "World!"},
			finalErr: io.EOF,
		}, {
			name: "final packet with extra packets buffered",
			blocks: map[int64]*simpleStreamingMessage{
				0: {
					StreamFinalPacket: "EOF",
					Message: wrp.Message{
						Payload: []byte("Done"),
					},
				},
				1: {
					Message: wrp.Message{
						Payload: []byte("Should not see this"),
					},
				},
				2: {
					Message: wrp.Message{
						Payload: []byte("Or this"),
					},
				},
			},
			bufSize:  10,
			expected: []string{"Done"},
			finalErr: io.EOF,
		}, {
			name: "incomplete block",
			blocks: map[int64]*simpleStreamingMessage{
				0: {
					StreamFinalPacket: "EOF",
					Message: wrp.Message{
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
					Message: wrp.Message{
						Payload: []byte("Hello"),
					},
				},
			},
			bufSize:  10,
			expected: []string{"Hello"},
			finalErr: io.ErrUnexpectedEOF,
		}, {
			name: "final packet larger than buffer - multiple reads required",
			blocks: map[int64]*simpleStreamingMessage{
				0: {
					StreamFinalPacket: "EOF",
					Message: wrp.Message{
						Payload: []byte("ABCDEFGHIJKLMNOP"), // 16 bytes
					},
				},
			},
			bufSize:  5, // Small buffer forces multiple reads
			expected: []string{"ABCDE", "FGHIJ", "KLMNO", "P"},
			finalErr: io.EOF,
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable for parallel subtest
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assembler := Assembler{}
			assembler.init()
			for k, v := range tt.blocks {
				assembler.packets[k] = v
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

			// Verify no packets remain after completion
			assert.Empty(t, assembler.packets, "all packets should be cleaned up after stream ends")
		})
	}
}

func TestAssembler_Close(t *testing.T) {
	assembler := &Assembler{
		packets: map[int64]*simpleStreamingMessage{
			1: {
				StreamFinalPacket: "EOF",
				Message: wrp.Message{
					Payload: []byte("Hello"),
				},
			},
		},
	}

	err := assembler.Close()
	assert.NoError(t, err)
	assert.True(t, assembler.closed)
}

func TestAssembler_CloseWithGap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		current     int64
		packets     map[int64]*simpleStreamingMessage
		expectErr   error
		expectFinal string
		description string
	}{
		{
			name:    "close with gap - buffered packets exist",
			current: 0,
			packets: map[int64]*simpleStreamingMessage{
				5: {
					Message: wrp.Message{
						Payload: []byte("unreachable"),
					},
				},
				10: {
					Message: wrp.Message{
						Payload: []byte("also unreachable"),
					},
				},
			},
			expectErr:   io.ErrUnexpectedEOF,
			expectFinal: "missing packet",
			description: "should report unexpected EOF when packets are buffered but current is missing",
		},
		{
			name:        "close without gap - no buffered packets",
			current:     5,
			packets:     map[int64]*simpleStreamingMessage{},
			expectErr:   io.EOF,
			expectFinal: "EOF",
			description: "should report normal EOF when no packets buffered",
		},
		{
			name:    "close with explicit final state already set",
			current: 0,
			packets: map[int64]*simpleStreamingMessage{
				5: {
					Message: wrp.Message{
						Payload: []byte("unreachable"),
					},
				},
			},
			expectErr:   io.ErrUnexpectedEOF,
			expectFinal: "custom error",
			description: "should use existing final state if already set",
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable for parallel subtest
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assembler := &Assembler{
				current: tt.current,
			}
			assembler.init()
			assembler.packets = tt.packets

			if tt.expectFinal == "custom error" {
				assembler.final = newUnexpectedEOF("custom error")
			}

			_ = assembler.Close()

			buf := make([]byte, 100)
			n, err := assembler.Read(buf)

			assert.Equal(t, 0, n, "should not read any bytes")
			assert.ErrorIs(t, err, tt.expectErr, tt.description)

			// Verify packets were cleaned up
			assert.Empty(t, assembler.packets, "buffered packets should be cleared")
		})
	}
}

func TestAssembler_ProcessWRP_PacketGap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		maxPacketGap int
		current      int64
		packetNumber int64
		expectError  bool
	}{
		{
			name:         "gap within limit",
			maxPacketGap: 10,
			current:      5,
			packetNumber: 10,
			expectError:  false,
		},
		{
			name:         "gap at exact limit",
			maxPacketGap: 10,
			current:      5,
			packetNumber: 15,
			expectError:  false,
		},
		{
			name:         "gap exceeds limit",
			maxPacketGap: 10,
			current:      5,
			packetNumber: 16,
			expectError:  true,
		},
		{
			name:         "no gap limit",
			maxPacketGap: 0,
			current:      5,
			packetNumber: 10000,
			expectError:  false,
		},
		{
			name:         "large gap with limit",
			maxPacketGap: 100,
			current:      0,
			packetNumber: 1000,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable for parallel subtest
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assembler := &Assembler{
				MaxPacketGap: tt.maxPacketGap,
				current:      tt.current,
			}
			assembler.init()

			msg := wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "mac:112233445566",
				Destination: "event:status/mac:112233445566",
				Headers: []string{
					"stream-id: 1",
					fmt.Sprintf("stream-packet-number: %d", tt.packetNumber),
				},
				Payload: []byte("test"),
			}

			err := assembler.ProcessWRP(context.Background(), msg)

			if tt.expectError {
				assert.ErrorIs(t, err, ErrPacketGapExceeded)
				assert.Empty(t, assembler.packets, "packet should not be buffered when gap exceeded")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestAssembler_ProcessWRP(t *testing.T) {
	t.Parallel()

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
					Message: wrp.Message{
						Type:        wrp.SimpleEventMessageType,
						Source:      "mac:112233445566",
						Destination: "event:status/mac:112233445566",
						Payload:     []byte("Hello"),
					},
				},
			},
			err: nil,
		}, {
			name: "valid message, custom validators",
			assembler: &Assembler{
				Validators: []wrp.Processor{
					wrp.NoStandardValidation(),
				},
			},
			msg: wrp.Message{
				Source:      "mac:112233445566",
				Destination: "event:status/mac:112233445566",
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
					Message: wrp.Message{
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
					Message: wrp.Message{
						Type:        wrp.SimpleEventMessageType,
						Source:      "mac:112233445566",
						Destination: "event:status/mac:112233445566",
						Payload:     []byte("Hello"),
					},
				},
			},
			err: nil,
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
						Message: wrp.Message{
							Type:        wrp.SimpleEventMessageType,
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
					Message: wrp.Message{
						Type:        wrp.SimpleEventMessageType,
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
						Message: wrp.Message{
							Type:        wrp.SimpleEventMessageType,
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
					Message: wrp.Message{
						Type:        wrp.SimpleEventMessageType,
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
						Message: wrp.Message{
							Type:        wrp.SimpleEventMessageType,
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
					Message: wrp.Message{
						Type:        wrp.SimpleEventMessageType,
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
		tt := tt // Capture range variable for parallel subtest
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			this := Assembler{}
			this.init()
			if tt.assembler != nil {
				this.Validators = tt.assembler.Validators
				this.closed = tt.assembler.closed
				this.current = tt.assembler.current
				if tt.assembler.packets != nil {
					this.packets = tt.assembler.packets
				}
			}

			err := this.ProcessWRP(ctx, tt.msg)
			assert.ErrorIs(t, err, tt.err)
			assert.Equal(t, tt.expected, this.packets)
		})
	}
}
