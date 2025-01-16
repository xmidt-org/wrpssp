// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnexpectedEOF_Error(t *testing.T) {
	err := &unexpectedEOF{message: "custom message"}
	expected := "unexpected EOF: custom message"
	assert.Equal(t, expected, err.Error())
}

func TestUnexpectedEOF_Is(t *testing.T) {
	err := &unexpectedEOF{message: "custom message"}
	assert.True(t, errors.Is(err, io.ErrUnexpectedEOF))
}

func TestUnexpectedEOF_Unwrap(t *testing.T) {
	err := &unexpectedEOF{message: "custom message"}
	unwrapped := err.Unwrap()
	assert.Len(t, unwrapped, 2)
	assert.Equal(t, io.ErrUnexpectedEOF, unwrapped[0])
	assert.Equal(t, "custom message", unwrapped[1].Error())
}
