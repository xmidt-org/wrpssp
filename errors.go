// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import (
	"errors"
	"fmt"
	"io"
)

var (
	ErrInvalidInput = errors.New("invalid input")
	ErrClosed       = errors.New("closed")
)

type unexpectedEOF struct {
	message string
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
		errors.New(e.message),
	}
}
