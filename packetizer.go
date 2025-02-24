// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import (
	"context"
	"errors"
	"io"

	"github.com/xmidt-org/wrp-go/v5"
)

// Packetizer is a struct that reads from a stream and populates a stream of
// WRP messages.
type Packetizer struct {
	headers       headers
	stream        io.Reader
	maxPacketSize int
	outcome       error
}

// New creates a new Packetizer with the given options.
func New(opts ...Option) (*Packetizer, error) {
	var file Packetizer

	defaults := []Option{
		MaxPacketSize(0),
		ReaderLength(0),
	}

	vadors := []Option{
		finalize(),
	}

	opts = append(defaults, opts...)
	opts = append(opts, vadors...)

	for _, opt := range opts {
		if err := opt.apply(&file); err != nil {
			return nil, err
		}
	}

	return &file, nil
}

// Next reads from the stream and populates the given WRP message.  The function
// will return the populated WRP message or an error.  The error io.EOF will
// be returned when the stream is exhausted.  Other errors may be returned if
// those are encountered during the processing.
func (p *Packetizer) Next(ctx context.Context) (wrp.Message, error) {
	if p.outcome != nil {
		return wrp.Message{}, p.outcome
	}

	buf := make([]byte, p.maxPacketSize)
	var err error
	var n int
	for n == 0 && err == nil {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			p.headers.finalPacket = err.Error()
		default:
			n, err = p.stream.Read(buf)
		}
	}

	msg := wrp.Message{
		Type: wrp.SimpleEventMessageType,
	}

	if n > 0 {
		msg.Payload = buf[:n]
	}

	if err != nil {
		if errors.Is(err, io.EOF) {
			p.headers.finalPacket = "EOF"
		} else {
			p.headers.finalPacket = err.Error()
		}
		p.outcome = err
	}

	p.headers.set(&msg)

	p.headers.currentPacketNumber++

	return msg, err
}
