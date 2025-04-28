// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xmidt-org/wrp-go/v5"
)

func TestSimpleStreamingMessage_From(t *testing.T) {
	tests := []struct {
		name    string
		msg     wrp.Message
		wantSSM simpleStreamingMessage
		wantErr error
	}{
		{
			name: "Valid SSP Message",
			msg: wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "self:/service",
				Destination: "event:foo",
				Headers: []string{
					"stream-id: test-stream-id",
					"stream-packet-number: 1",
					"stream-estimated-total-length: 100",
					"stream-final-packet: eof",
					"stream-encoding: gzip",
					"unrelated: header",
					"header with no colon",
				},
			},
			wantSSM: simpleStreamingMessage{
				Message: wrp.Message{
					Type:        wrp.SimpleEventMessageType,
					Source:      "self:/service",
					Destination: "event:foo",
					Headers: []string{
						"unrelated: header",
						"header with no colon",
					},
				},
				StreamID:              "test-stream-id",
				StreamPacketNumber:    1,
				StreamEstimatedLength: 100,
				StreamFinalPacket:     "eof",
				StreamEncoding:        EncodingGzip,
			},
			wantErr: nil,
		}, {
			name: "Invalid SSP Headers, missing stream-id",
			msg: wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "self:/service",
				Destination: "event:foo",
				Headers: []string{
					"stream-packet-number: 0000",
					"stream-estimated-total-length: 0",
					"stream-final-packet: eof",
					"stream-encoding: gzip",
				},
			},
			wantErr: ErrInvalidInput,
		}, {
			name: "Invalid numbers in headers",
			msg: wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "self:/service",
				Destination: "event:foo",
				Headers: []string{
					"stream-packet-number: invalid",
				},
			},
			wantErr: ErrInvalidInput,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ssm simpleStreamingMessage
			err := ssm.From(&tt.msg)

			if err == nil {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantSSM, ssm)
				return
			}

			assert.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestSimpleStreamingMessage_InnerFrom(t *testing.T) {
	tests := []struct {
		name    string
		headers map[string]string
		want    simpleStreamingMessage
		err     error
	}{
		{
			name: "Valid SSP Headers",
			headers: map[string]string{
				stream_id:               "test-stream-id",
				stream_packet_number:    "1",
				stream_estimated_length: "100",
				stream_final_packet:     "eof",
				stream_encoding:         "gzip",
			},
			want: simpleStreamingMessage{
				StreamID:              "test-stream-id",
				StreamPacketNumber:    1,
				StreamEstimatedLength: 100,
				StreamFinalPacket:     "eof",
				StreamEncoding:        EncodingGzip,
			},
			err: nil,
		}, {
			name: "Valid SSP Headers, extra zeros",
			headers: map[string]string{
				stream_packet_number:    "0000",
				stream_estimated_length: "0000100",
			},
			want: simpleStreamingMessage{
				StreamPacketNumber:    0,
				StreamEstimatedLength: 100,
			},
			err: nil,
		}, {
			name:    "Validate invalid/default values",
			headers: map[string]string{},
			want: simpleStreamingMessage{
				StreamID:              "",
				StreamPacketNumber:    -1,
				StreamEstimatedLength: 0,
				StreamFinalPacket:     "",
				StreamEncoding:        "",
			},
		}, {
			name: "Alternative final packet name",
			headers: map[string]string{
				stream_final_packet: "Somthing Else",
			},
			want: simpleStreamingMessage{
				StreamPacketNumber: -1,
				StreamFinalPacket:  "Somthing Else",
			},
		}, {
			name: "empty estimated length",
			headers: map[string]string{
				stream_estimated_length: "",
			},
			want: simpleStreamingMessage{
				StreamPacketNumber:    -1,
				StreamEstimatedLength: 0,
			},
		}, {
			name: "empty packet number",
			headers: map[string]string{
				stream_packet_number: "",
			},
			want: simpleStreamingMessage{
				StreamPacketNumber: -1,
			},
		}, {
			name: "Alternative EOF",
			headers: map[string]string{
				stream_final_packet: "EOF",
			},
			want: simpleStreamingMessage{
				StreamPacketNumber: -1,
				StreamFinalPacket:  "eof",
			},
		}, {
			name: "Negative StreamPacketNumber",
			headers: map[string]string{
				stream_packet_number: "-12",
			},
			want: simpleStreamingMessage{
				StreamPacketNumber: -12,
			},
		}, {
			name: "Invalid StreamPacketNumber",
			headers: map[string]string{
				stream_packet_number: "invalid",
			},
			want: simpleStreamingMessage{},
			err:  ErrInvalidInput,
		}, {
			name: "Invalid StreamPacketNumber, float",
			headers: map[string]string{
				stream_packet_number: "12.2",
			},
			want: simpleStreamingMessage{},
			err:  ErrInvalidInput,
		}, {
			name: "Invalid StreamEstimatedLength",
			headers: map[string]string{
				stream_estimated_length: "invalid",
			},
			want: simpleStreamingMessage{},
			err:  ErrInvalidInput,
		}, {
			name: "Invalid StreamEstimatedLength, float",
			headers: map[string]string{
				stream_estimated_length: "12.2",
			},
			want: simpleStreamingMessage{},
			err:  ErrInvalidInput,
		}, {
			name: "Invalid StreamEstimatedLength, negative",
			headers: map[string]string{
				stream_estimated_length: "-12",
			},
			want: simpleStreamingMessage{},
			err:  ErrInvalidInput,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ssm simpleStreamingMessage
			err := ssm.from(tt.headers)

			if tt.err == nil {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, ssm)
				return
			}

			assert.Error(t, err)
			assert.ErrorIs(t, err, tt.err)
		})
	}
}

func TestSimpleStreamingMessage_To(t *testing.T) {
	tests := []struct {
		name    string
		ssm     simpleStreamingMessage
		wantMsg wrp.Message
		wantErr error
	}{
		{
			name: "Valid SSP Message",
			ssm: simpleStreamingMessage{
				Message: wrp.Message{
					Type:        wrp.SimpleEventMessageType,
					Source:      "self:/service",
					Destination: "event:foo",
				},
				StreamID:              "test-stream-id",
				StreamPacketNumber:    1,
				StreamEstimatedLength: 100,
				StreamFinalPacket:     "eof",
				StreamEncoding:        EncodingGzip,
			},
			wantMsg: wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "self:/service",
				Destination: "event:foo",
				Headers: []string{
					"stream-id: test-stream-id",
					"stream-packet-number: 1",
					"stream-estimated-total-length: 100",
					"stream-final-packet: eof",
					"stream-encoding: gzip",
				},
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var msg wrp.Message
			err := tt.ssm.To(&msg)
			assert.Equal(t, tt.wantErr, err)
			assert.Equal(t, tt.wantMsg, msg)
		})
	}
}

func TestSimpleStreamingMessage_Validate(t *testing.T) {
	tests := []struct {
		name       string
		ssm        simpleStreamingMessage
		validators []wrp.Processor
		wantErr    error
	}{
		{
			name: "Valid SSP Message",
			ssm: simpleStreamingMessage{
				Message: wrp.Message{
					Type:        wrp.SimpleEventMessageType,
					Source:      "self:/service",
					Destination: "event:foo",
				},
				StreamID:              "test-stream-id",
				StreamPacketNumber:    1,
				StreamEstimatedLength: 100,
				StreamFinalPacket:     "eof",
				StreamEncoding:        EncodingGzip,
			},
			wantErr: nil,
		}, {
			name: "Missing StreamID",
			ssm: simpleStreamingMessage{
				Message: wrp.Message{
					Type:        wrp.SimpleEventMessageType,
					Source:      "self:/service",
					Destination: "event:foo",
				},
				StreamPacketNumber:    1,
				StreamEstimatedLength: 100,
				StreamFinalPacket:     "eof",
				StreamEncoding:        EncodingGzip,
			},
			wantErr: ErrInvalidInput,
		}, {
			name: "Negative StreamPacketNumber",
			ssm: simpleStreamingMessage{
				Message: wrp.Message{
					Type:        wrp.SimpleEventMessageType,
					Source:      "self:/service",
					Destination: "event:foo",
				},
				StreamID:              "test-stream-id",
				StreamPacketNumber:    -1,
				StreamEstimatedLength: 100,
				StreamFinalPacket:     "eof",
				StreamEncoding:        EncodingGzip,
			},
			wantErr: ErrInvalidInput,
		}, {
			name: "Invalid StreamEncoding",
			ssm: simpleStreamingMessage{
				Message: wrp.Message{
					Type:        wrp.SimpleEventMessageType,
					Source:      "self:/service",
					Destination: "event:foo",
				},
				StreamID:              "test-stream-id",
				StreamPacketNumber:    1,
				StreamEstimatedLength: 100,
				StreamFinalPacket:     "eof",
				StreamEncoding:        "invalid",
			},
			wantErr: ErrInvalidInput,
		}, {
			name: "Invalid Message - missing Destination",
			ssm: simpleStreamingMessage{
				Message: wrp.Message{
					Type:   wrp.SimpleEventMessageType,
					Source: "self:/service",
				},
				StreamID:           "test-stream-id",
				StreamPacketNumber: 1,
			},
			wantErr: wrp.ErrMessageIsInvalid,
		}, {
			name: "Invalid Message - missing Destination",
			ssm: simpleStreamingMessage{
				Message: wrp.Message{
					Type:   wrp.SimpleEventMessageType,
					Source: "self:/service",
				},
			},
			validators: []wrp.Processor{
				wrp.NoStandardValidation(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.ssm.Validate(tt.validators...)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestSimpleStreamingMsgType(t *testing.T) {
	ssm := simpleStreamingMessage{}
	assert.Equal(t, wrp.SimpleEventMessageType, ssm.MsgType())
}

func TestEncodingIsValid(t *testing.T) {
	tests := []struct {
		name string
		e    Encoding
		want bool
	}{
		{
			name: "Valid Encoding identity",
			e:    EncodingIdentity,
			want: true,
		}, {
			name: "Valid Encoding gzip",
			e:    EncodingGzip,
			want: true,
		}, {
			name: "Valid Encoding deflate",
			e:    EncodingDeflate,
			want: true,
		}, {
			name: "Valid Encoding empty",
			e:    "",
			want: true,
		}, {
			name: "Invalid Encoding",
			e:    "invalid",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.e.isValid())
		})
	}
}

func TestEncodingIs(t *testing.T) {
	tests := []struct {
		name string
		e    Encoding
		is   Encoding
		want bool
	}{
		{
			name: "Valid Encoding identity",
			e:    EncodingIdentity,
			is:   EncodingIdentity,
			want: true,
		}, {
			name: "Valid Encoding gzip",
			e:    EncodingGzip,
			is:   EncodingGzip,
			want: true,
		}, {
			name: "Valid Encoding deflate",
			e:    EncodingDeflate,
			is:   EncodingDeflate,
			want: true,
		}, {
			name: "Valid Encoding empty",
			e:    "",
			is:   EncodingIdentity,
			want: true,
		}, {
			name: "Invalid Encoding",
			e:    "invalid",
			is:   EncodingIdentity,
			want: false,
		}, {
			name: "Invalid Encoding",
			e:    EncodingIdentity,
			is:   "invalid",
			want: false,
		}, {
			name: "Invalid, invalid Encoding",
			e:    "invalid",
			is:   "invalid",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.e.is(tt.is))
		})
	}
}

func TestMessageIs(t *testing.T) {
	tests := []struct {
		name     string
		msg      wrp.Union
		expected bool
	}{
		{
			name: "SimpleStreamingMessage",
			msg: &simpleStreamingMessage{
				StreamID: "test-stream-id",
			},
			expected: true,
		},
		{
			name: "Valid Message with Headers",
			msg: &wrp.Message{
				Headers: []string{"stream-id: test-stream-id"},
			},
			expected: true,
		},
		{
			name: "Valid Message without Headers",
			msg: &wrp.Message{
				Headers: []string{},
			},
			expected: false,
		},
		{
			name:     "Nil Message",
			msg:      nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ssm simpleStreamingMessage
			result := ssm.Is(tt.msg)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetStreamID(t *testing.T) {
	someErr := fmt.Errorf("some error")
	tests := []struct {
		name    string
		msg     wrp.Message
		wantID  string
		wantErr error
	}{
		{
			name: "Valid SSP Message",
			msg: wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "mac:112233445566",
				Destination: "event:status/mac:112233445566",
				Headers: []string{
					"stream-id:Test-Stream-Id",
					"stream-packet-number:0",
				},
			},
			wantID: "Test-Stream-Id",
		}, {
			name: "Non-SSP Message",
			msg: wrp.Message{
				Type:        wrp.SimpleRequestResponseMessageType,
				Source:      "mac:112233445566",
				Destination: "event:status/mac:112233445566",
			},
			wantErr: wrp.ErrNotHandled,
		}, {
			name: "A SSP message without the stream-pack-number is ok",
			msg: wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "mac:112233445566",
				Destination: "event:status/mac:112233445566",
				Headers: []string{
					"stream-id:Test-Stream-Id",
				},
			},
			wantID: "Test-Stream-Id",
		}, {
			name: "SSP Message Without Stream ID",
			msg: wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "mac:112233445566",
				Destination: "event:status/mac:112233445566",
			},
			wantErr: wrp.ErrNotHandled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotID, err := GetStreamID(tt.msg)

			if tt.wantErr != nil {
				assert.Error(t, err)
				if !errors.Is(tt.wantErr, someErr) {
					assert.ErrorIs(t, err, tt.wantErr)
				}
				assert.Empty(t, gotID)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.wantID, gotID)
		})
	}
}
