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
	stream              io.Reader
	id                  string
	currentPacketNumber int64
	maxPacketSize       int
	encoding            Encoding
	txGen               func() (string, error)
	estimatedSize       uint64
	finalPacket         string
	outcome             error
}

// New creates a new Packetizer with the given options.  Similar to io.Reader and
// io.Writer, the Packetizer is not safe for concurrent use.
func New(opts ...Option) (*Packetizer, error) {
	var file Packetizer

	defaults := []Option{
		MaxPacketSize(0),
		EstimatedLength(0),
		WithEncoding(EncodingGzip),
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

// Next reads from the byte stream and populates the provided WRP message struct
// with a new payload and additional SSP headers.  The function will return the
// populated WRP message or an error.  The error io.EOF will be returned when
// the stream is exhausted.  Other errors may be returned if those are
// encountered during the processing.
func (p *Packetizer) Next(ctx context.Context, msg wrp.Message, validators ...wrp.Processor) (*wrp.Message, error) {
	ssm, err := p.nextRaw(ctx, msg)
	if ssm == nil {
		return nil, err
	}

	var out wrp.Message
	if err := ssm.To(&out, validators...); err != nil {
		return nil, err
	}

	return &out, err
}

func (p *Packetizer) nextRaw(ctx context.Context, msg ...wrp.Message) (*simpleStreamingMessage, error) {
	if p.outcome != nil {
		return nil, p.outcome
	}

	buf := make([]byte, p.maxPacketSize)
	var err error
	var n int
	for n == 0 && err == nil {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			p.finalPacket = err.Error()
		default:
			n, err = p.stream.Read(buf)
		}
	}

	if err != nil {
		if errors.Is(err, io.EOF) {
			p.finalPacket = "EOF"
		} else {
			p.finalPacket = err.Error()
		}
		p.outcome = err
	}

	var out simpleStreamingMessage

	if len(msg) > 0 {
		out.Message = msg[0]
	}

	out.Payload = nil
	if n > 0 {
		out.Payload, err = p.encoding.encode(buf[:n])
		if err != nil {
			p.finalPacket = err.Error()
			p.outcome = err
		}
	}

	out.StreamID = p.id
	out.StreamPacketNumber = p.currentPacketNumber
	out.StreamEstimatedLength = p.estimatedSize
	out.StreamFinalPacket = p.finalPacket
	out.StreamEncoding = p.encoding

	p.currentPacketNumber++

	if p.txGen != nil {
		txID, err := p.txGen()
		if err != nil {
			return nil, err
		}
		out.TransactionUUID = txID
	}

	return &out, p.outcome
}
