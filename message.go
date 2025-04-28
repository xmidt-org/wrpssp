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

// simpleStreamingMessage is a WRP message that contains the necessary fields
// and methods to support streaming.  Normal interactions with this message
// should be through the Packetizer and Assember interfaces in this package.
type simpleStreamingMessage struct {
	wrp.Message
	StreamID              string
	StreamPacketNumber    int64
	StreamEstimatedLength uint64
	StreamFinalPacket     string
	StreamEncoding        Encoding
}

var _ wrp.Union = &simpleStreamingMessage{}

func (ssm *simpleStreamingMessage) From(msg *wrp.Message, validators ...wrp.Processor) error {
	err := ssm.Message.From(msg, wrp.NoStandardValidation())
	if err != nil {
		return err
	}

	mine, others := split(msg.Headers)
	if len(others) == 0 {
		ssm.Headers = nil
	} else {
		ssm.Headers = others
	}
	if err := ssm.from(mine); err != nil {
		return err
	}

	return ssm.Validate(validators...)
}

func (ssm *simpleStreamingMessage) MsgType() wrp.MessageType {
	return wrp.SimpleEventMessageType
}

func (ssm *simpleStreamingMessage) To(msg *wrp.Message, validators ...wrp.Processor) error {
	if err := ssm.Validate(validators...); err != nil {
		return err
	}

	_ = ssm.Message.To(msg, wrp.NoStandardValidation())

	_, others := split(ssm.Headers)
	ours := ssm.headers()

	msg.Headers = append(others, ours...)

	return nil
}

func (ssm *simpleStreamingMessage) Validate(validators ...wrp.Processor) error {
	if err := ssm.Message.Validate(validators...); err != nil {
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
	if !ssm.StreamEncoding.isValid() {
		errs = append(errs, errors.New("StreamEncoding must be one of identity, gzip, or deflate"))
	}

	if len(errs) == 0 {
		return nil
	}

	errs = append(errs, ErrInvalidInput)

	return errors.Join(errs...)
}

func (ssm *simpleStreamingMessage) Is(msg wrp.Union) bool {
	return Is(msg, wrp.NoStandardValidation())
}

// from takes a map of headers and sets the values of the SimpleStreamingMessage
// based on the values in the map.  The only errors returned are if integer values
// cannot be parsed.
func (ssm *simpleStreamingMessage) from(headers map[string]string) error {
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
				ssm.StreamFinalPacket = tmp
			} else {
				ssm.StreamFinalPacket = value
			}
		case stream_encoding:
			ssm.StreamEncoding = Encoding(value)
		}
	}

	return nil
}

func (ssm *simpleStreamingMessage) headers() []string {
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

	if ssm.StreamEncoding.isValid() && !ssm.StreamEncoding.is(EncodingIdentity) {
		headers = append(headers, stream_encoding+": "+ssm.StreamEncoding.string())
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

// GetStreamID returns the stream ID of the message if it is an SSP message.
// There is only minimal validation done since this is a function used to
// sort messages quickly.
func GetStreamID(msg wrp.Message) (string, error) {
	if msg.Type != wrp.SimpleEventMessageType {
		return "", wrp.ErrNotHandled
	}

	mine, _ := split(msg.Headers)
	if id, ok := mine[stream_id]; ok {
		return id, nil
	}

	return "", wrp.ErrNotHandled
}
