// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import (
	"errors"
	"fmt"
	"io"
)

var (
	ErrInvalidInput      = errors.New("invalid input")
	ErrClosed            = errors.New("closed")
	ErrPacketGapExceeded = errors.New("packet gap exceeded")
)

type unexpectedEOF struct {
	message    string
	messageErr error
}

func (e *unexpectedEOF) Error() string {
	return fmt.Sprintf("%s: %s", io.ErrUnexpectedEOF.Error(), e.message)
}

func (e *unexpectedEOF) Is(target error) bool {
	return errors.Is(target, io.ErrUnexpectedEOF)
}

func (e *unexpectedEOF) Unwrap() []error {
	return []error{
		io.ErrUnexpectedEOF,
		e.messageErr,
	}
}

// newUnexpectedEOF creates a new unexpectedEOF error with the given message.
func newUnexpectedEOF(message string) *unexpectedEOF {
	return &unexpectedEOF{
		message:    message,
		messageErr: errors.New(message),
	}
}

type packetGapExceeded struct {
	current  int64
	received int64
	maxGap   int
}

func (e *packetGapExceeded) Error() string {
	return fmt.Sprintf("%s: received packet %d while at %d (gap %d > max %d)",
		ErrPacketGapExceeded.Error(), e.received, e.current, e.received-e.current, e.maxGap)
}

func (e *packetGapExceeded) Is(target error) bool {
	return errors.Is(target, ErrPacketGapExceeded)
}

func (e *packetGapExceeded) Unwrap() error {
	return ErrPacketGapExceeded
}
