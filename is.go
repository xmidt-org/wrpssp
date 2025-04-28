// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import "github.com/xmidt-org/wrp-go/v5"

// Is determines if the given message is a Simple Streaming Protocol message.
// If wrp.NoStandardValidation() is passed as a validator, then the message is
// considered a Simple Streaming Protocol message if it has any matching headers.
func Is(msg wrp.Union, validators ...wrp.Processor) bool {
	// If the message is already a SimpleStreamingMessage, then it is this type.
	if _, ok := msg.(*simpleStreamingMessage); ok {
		return true
	}

	tmp, ok := msg.(*wrp.Message)
	if !ok {
		tmp = new(wrp.Message)
		if err := wrp.As(msg, tmp, validators...); err != nil {
			return false
		}
	}

	mine, _ := split(tmp.Headers)

	// If any headers match, then this is a message of this type.
	return len(mine) > 0
}
