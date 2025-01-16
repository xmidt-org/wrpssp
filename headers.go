// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/xmidt-org/wrp-go/v3"
)

const (
	// These are the string literals found in the messages.

	stream_id               = "stream-id"
	stream_packet_number    = "stream-packet-number"
	stream_estimated_length = "stream-estimated-total-length"
	stream_final_packet     = "stream-final-packet"
)

// headers provides an easy way to consistently set, get, and validate the headers.
type headers struct {
	id                  string
	currentPacketNumber int64
	totalLength         uint64
	finalPacket         string
}

func (h headers) set(msg *wrp.Message) {
	if msg.Headers == nil {
		msg.Headers = make([]string, 0, 1)
	}

	clearExisting(msg)

	setS(msg, stream_id, h.id)
	setI(msg, stream_packet_number, h.currentPacketNumber)
	if h.totalLength > 0 {
		setU(msg, stream_estimated_length, h.totalLength)
	}
	if h.finalPacket != "" {
		setS(msg, stream_final_packet, h.finalPacket)
	}
}

func get(msg *wrp.Message) (headers, error) {
	h := headers{
		// default the currentPacketNumber to -1 to indicate that it is not set
		currentPacketNumber: -1,
	}

	for _, header := range msg.Headers {
		key, value, found := strings.Cut(header, ":")
		if !found {
			continue
		}

		key = strings.TrimSpace(key)
		key = strings.ToLower(key)

		value = strings.TrimSpace(value)

		switch key {
		case stream_id:
			h.id = value
		case stream_packet_number:
			i, err := getI(value)
			if err != nil {
				return headers{}, err
			}
			h.currentPacketNumber = i
		case stream_estimated_length:
			i, err := getU(value)
			if err != nil {
				return headers{}, err
			}
			h.totalLength = i
		case stream_final_packet:
			// we don't care about the value, just the presence of the header
			if value == "" {
				value = "EOF"
			}
			h.finalPacket = value
		}
	}

	if err := h.isValid(); err != nil {
		return headers{}, err
	}

	return h, nil
}

func isSSP(msg *wrp.Message) bool {
	if msg.MessageType() == wrp.SimpleEventMessageType {
		for _, header := range msg.Headers {
			header = strings.TrimSpace(header)
			header = strings.ToLower(header)
			if strings.HasPrefix(header, stream_id) {
				return true
			}
		}
	}
	return false
}

func (h headers) isValid() error {
	if h.id == "" {
		return fmt.Errorf("%w: id must not be empty", ErrInvalidInput)
	}
	if h.currentPacketNumber < 0 {
		return fmt.Errorf("%w: currentPacketNumber must be set", ErrInvalidInput)
	}

	return nil
}

func getU(s string) (uint64, error) {
	if s == "0" {
		return 0, nil
	}

	s = strings.TrimLeft(s, "0")

	return strconv.ParseUint(s, 10, 64)
}

// getI is a helper function to parse an int64 from a string.  It trims leading zeros.
// If the string is "0", it returns 0.  Negative numbers are not supported because
// the stream_packet_number is unsigned.
func getI(s string) (int64, error) {
	if s == "0" {
		return 0, nil
	}

	s = strings.TrimLeft(s, "0")

	return strconv.ParseInt(s, 10, 64)
}

func setI(msg *wrp.Message, key string, value int64) {
	s := strconv.FormatInt(value, 10)
	setS(msg, key, s)
}

func setU(msg *wrp.Message, key string, value uint64) {
	s := strconv.FormatUint(value, 10)
	setS(msg, key, s)
}

func setS(msg *wrp.Message, key, value string) {
	msg.Headers = append(msg.Headers, key+": "+value)
}

var headerList = []string{
	stream_id,
	stream_packet_number,
	stream_estimated_length,
	stream_final_packet,
}

func clearExisting(msg *wrp.Message) {
	replacements := make([]string, 0, len(msg.Headers))
	for _, header := range msg.Headers {
		s := strings.TrimSpace(header)
		s = strings.ToLower(s)

		var found bool
		for _, key := range headerList {
			if strings.HasPrefix(s, key) {
				found = true
				break
			}
		}

		if !found {
			replacements = append(replacements, header)
		}
	}
	msg.Headers = replacements
}
