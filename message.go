// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import (
	"errors"
	"strconv"
	"strings"

	"github.com/xmidt-org/wrp-go/v5"
)

const (
	// These are the string literals found in the messages.
	stream_id               = "stream-id"
	stream_packet_number    = "stream-packet-number"
	stream_estimated_length = "stream-estimated-total-length"
	stream_final_packet     = "stream-final-packet"
	stream_encoding         = "stream-encoding"
)

const (
	EncodingIdentity Encoding = "identity"
	EncodingGzip     Encoding = "gzip"
	EncodingDeflate  Encoding = "deflate"
)

type Encoding string

func (e Encoding) IsValid() bool {
	switch e {
	case "", EncodingIdentity, EncodingGzip, EncodingDeflate:
		return true
	default:
		return false
	}
}

func (e Encoding) Is(want Encoding) bool {
	if want == EncodingIdentity {
		return e == "" || e == EncodingIdentity
	}

	return e == want && e.IsValid()
}

type SimpleStreamingMessage struct {
	wrp.SimpleEvent
	StreamID              string
	StreamPacketNumber    int64
	StreamEstimatedLength uint64
	StreamFinalPacket     string
	StreamEncoding        Encoding
}

var _ wrp.Union = &SimpleStreamingMessage{}

func (ssm *SimpleStreamingMessage) From(msg *wrp.Message, validators ...wrp.Processor) error {
	err := ssm.SimpleEvent.From(msg, wrp.NoStandardValidation())
	if err != nil {
		return err
	}

	mine, others := split(msg.Headers)
	ssm.SimpleEvent.Headers = others
	if err := ssm.from(mine); err != nil {
		return err
	}

	return ssm.Validate(validators...)
}

func (ssm *SimpleStreamingMessage) MsgType() wrp.MessageType {
	return wrp.SimpleEventMessageType
}

func (ssm *SimpleStreamingMessage) To(msg *wrp.Message, validators ...wrp.Processor) error {
	if err := ssm.Validate(validators...); err != nil {
		return err
	}

	_ = ssm.SimpleEvent.To(msg, wrp.NoStandardValidation())

	_, others := split(ssm.Headers)
	ours := ssm.headers()

	msg.Headers = append(others, ours...)

	return nil
}

func (ssm *SimpleStreamingMessage) Validate(validators ...wrp.Processor) error {
	if err := ssm.SimpleEvent.Validate(validators...); err != nil {
		return err
	}

	if wrp.SkipStandardValidation(validators) {
		return nil
	}

	errs := make([]error, 0, 3)

	if ssm.StreamID == "" {
		errs = append(errs, errors.New("StreamID is required"))
	}
	if ssm.StreamPacketNumber < 0 {
		errs = append(errs, errors.New("StreamPacketNumber must be non-negative"))
	}
	if !ssm.StreamEncoding.IsValid() {
		errs = append(errs, errors.New("StreamEncoding must be one of identity, gzip, or deflate"))
	}

	if len(errs) == 0 {
		return nil
	}

	errs = append(errs, ErrInvalidInput)

	return errors.Join(errs...)
}

// from takes a map of headers and sets the values of the SimpleStreamingMessage
// based on the values in the map.  The only errors returned are if integer values
// cannot be parsed.
func (ssm *SimpleStreamingMessage) from(headers map[string]string) error {
	// Set to invalid value to ensure that the value is set.
	ssm.StreamID = ""
	ssm.StreamPacketNumber = -1

	// Set to default values
	ssm.StreamEstimatedLength = 0
	ssm.StreamFinalPacket = ""
	ssm.StreamEncoding = ""
	for key, value := range headers {
		switch key {
		case stream_id:
			ssm.StreamID = value
		case stream_packet_number:
			zero, value := isZero(value)
			if zero {
				ssm.StreamPacketNumber = 0
				break
			}
			if value == "" {
				break
			}

			i, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return errors.Join(ErrInvalidInput, err)
			}
			ssm.StreamPacketNumber = i
		case stream_estimated_length:
			zero, value := isZero(value)
			if zero || value == "" {
				ssm.StreamEstimatedLength = 0
				break
			}

			i, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return errors.Join(ErrInvalidInput, err)
			}
			ssm.StreamEstimatedLength = i
		case stream_final_packet:
			tmp := strings.ToLower(value)
			if tmp == "eof" {
				ssm.StreamFinalPacket = "eof"
			} else {
				ssm.StreamFinalPacket = value
			}
		case stream_encoding:
			ssm.StreamEncoding = Encoding(value)
		}
	}

	return nil
}

func (ssm *SimpleStreamingMessage) headers() []string {
	headers := make([]string, 0, 5)

	if ssm.StreamID != "" {
		headers = append(headers, stream_id+": "+ssm.StreamID)
	}

	if ssm.StreamPacketNumber >= 0 {
		headers = append(headers, stream_packet_number+": "+strconv.FormatInt(ssm.StreamPacketNumber, 10))
	}

	if ssm.StreamEstimatedLength > 0 {
		headers = append(headers, stream_estimated_length+": "+strconv.FormatUint(ssm.StreamEstimatedLength, 10))
	}

	if ssm.StreamFinalPacket != "" {
		final := ssm.StreamFinalPacket
		if strings.ToLower(strings.TrimSpace(final)) == "eof" {
			final = "eof"
		}
		headers = append(headers, stream_final_packet+": "+final)
	}

	if ssm.StreamEncoding.IsValid() && !ssm.StreamEncoding.Is(EncodingIdentity) {
		headers = append(headers, stream_encoding+": "+string(ssm.StreamEncoding))
	}

	return headers
}

// treat the value as zero if it is empty or "0" or with any number of "0" characters
func isZero(s string) (bool, string) {
	if s == "" {
		return false, s
	}

	if s == "0" {
		return true, s
	}

	s = strings.TrimLeft(s, "0")

	return s == "", s
}

var headerKeys = map[string]struct{}{
	stream_id:               {},
	stream_packet_number:    {},
	stream_estimated_length: {},
	stream_final_packet:     {},
	stream_encoding:         {},
}

func split(headers []string) (map[string]string, []string) {
	result := make(map[string]string, len(headers))
	others := make([]string, 0, len(headers))

	for _, header := range headers {
		key, value, found := strings.Cut(header, ":")
		if !found {
			others = append(others, header)
			continue
		}

		key = strings.TrimSpace(key)
		key = strings.ToLower(key)

		if _, ok := headerKeys[key]; !ok {
			others = append(others, header)
			continue
		}

		result[key] = strings.TrimSpace(value)
	}

	return result, others
}
