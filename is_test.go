// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xmidt-org/wrp-go/v5"
)

func TestIs(t *testing.T) {
	tests := []struct {
		name       string
		msg        wrp.Union
		validators []wrp.Processor
		expected   bool
	}{
		{
			name: "SimpleStreamingMessage",
			msg: &simpleStreamingMessage{
				StreamID: "test-stream-id",
			},
			expected: true,
		}, {
			name: "Valid SimpleEvent with Headers",
			msg: &wrp.SimpleEvent{
				Source:      "mac:112233445566",
				Destination: "event:device-status",
				Headers:     []string{"stream-id: test-stream-id"},
			},
			expected: true,
		}, {
			name: "Valid SimpleEvent with Headers, and ignore validators",
			msg: &wrp.SimpleEvent{
				Headers: []string{"stream-id: test-stream-id"},
			},
			validators: []wrp.Processor{wrp.NoStandardValidation()},
			expected:   true,
		}, {
			name:     "Valid SimpleEvent without Headers",
			msg:      &wrp.SimpleEvent{},
			expected: false,
		}, {
			name: "Invalid Message Type",
			msg: &wrp.SimpleRequestResponse{
				Headers: []string{"stream-id: test-stream-id"},
			},
			expected: false,
		}, {
			name:     "Nil Message",
			msg:      nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Is(tt.msg, tt.validators...)
			assert.Equal(t, tt.expected, result)
		})
	}
}
