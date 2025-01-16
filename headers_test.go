// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xmidt-org/wrp-go/v3"
)

func TestGet(t *testing.T) {
	tests := []struct {
		name     string
		msg      wrp.Message
		expected headers
		err      error
	}{
		{
			name: "valid headers",
			msg: wrp.Message{
				Headers: []string{
					"stream-id: 123",
					"stream-packet-number: 1",
					"stream-estimated-total-length: 100",
					"stream-final-packet: done",
				},
			},
			expected: headers{
				id:                  "123",
				currentPacketNumber: 1,
				totalLength:         100,
				finalPacket:         "done",
			},
			err: nil,
		}, {
			name: "alternate valid headers",
			msg: wrp.Message{
				Headers: []string{
					"  stream-id  :    123     ",
					"stream-id     ignored     ",
					"Stream-Packet-Number: 0",
					"stream-estimated-total-length: 100",
					"stream-final-packet:",
				},
			},
			expected: headers{
				id:                  "123",
				currentPacketNumber: 0,
				totalLength:         100,
				finalPacket:         "EOF",
			},
			err: nil,
		},
		{
			name: "missing headers",
			msg: wrp.Message{
				Headers: []string{},
			},
			expected: headers{},
			err:      ErrInvalidInput,
		},
		{
			name: "invalid packet number",
			msg: wrp.Message{
				Headers: []string{
					"stream-id: 123",
					"stream-packet-number: abc",
				},
			},
			expected: headers{},
			err:      strconv.ErrSyntax,
		},
		{
			name: "invalid total length",
			msg: wrp.Message{
				Headers: []string{
					"stream-id: 123",
					"stream-estimated-total-length: abc",
				},
			},
			expected: headers{},
			err:      strconv.ErrSyntax,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, err := get(&tt.msg)
			if tt.err != nil {
				assert.ErrorIs(t, err, tt.err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, h)
			}
		})
	}
}

func TestSet(t *testing.T) {
	tests := []struct {
		name     string
		headers  headers
		start    wrp.Message
		expected wrp.Message
	}{
		{
			name: "all headers set",
			headers: headers{
				id:                  "123",
				currentPacketNumber: 1,
				totalLength:         100,
				finalPacket:         "done",
			},
			expected: wrp.Message{
				Headers: []string{
					"other-header: value",
					"stream-id: 123",
					"stream-packet-number: 1",
					"stream-estimated-total-length: 100",
					"stream-final-packet: done",
				},
			},
			start: wrp.Message{
				Headers: []string{
					"other-header: value",
					"stream-id",
					"stream-final-packet: none",
					"stream-final-packet: some",
					"stream-final-packet: 80 percent",
					"stream-final-packet: complete",
				},
			},
		},
		{
			name: "optional headers omitted",
			headers: headers{
				id:                  "123",
				currentPacketNumber: 1,
			},
			expected: wrp.Message{
				Headers: []string{
					"stream-id: 123",
					"stream-packet-number: 1",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.headers.set(&tt.start)
			assert.ElementsMatch(t, tt.expected.Headers, tt.start.Headers)
		})
	}
}
