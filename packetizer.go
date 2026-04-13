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
	outcome             error
}

// New creates a new Packetizer with the given options.  Similar to io.Reader and
// io.Writer, the Packetizer is not safe for concurrent use.
func New(opts ...Option) (*Packetizer, error) {
	var file Packetizer

	defaults := []Option{ // nolint:prealloc
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
	// Check outcome first so sticky stream state takes precedence over context
	if p.outcome != nil {
		return nil, p.outcome
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	var tx string
	var err error
	if p.txGen != nil {
		tx, err = p.txGen()
		if err != nil {
			p.outcome = err
			return nil, err
		}
	}

	ssm := p.nextRaw(ctx, tx, msg)

	var out wrp.Message
	if err := ssm.To(&out, validators...); err != nil {
		return nil, err
	}

	return &out, p.outcome
}

func (p *Packetizer) nextRaw(ctx context.Context, tx string, msg wrp.Message) *simpleStreamingMessage {
	buf, err := p.readChunk(ctx)

	var out simpleStreamingMessage

	out.Message = msg

	p.outcome = err

	out.StreamID = p.id
	out.StreamPacketNumber = p.currentPacketNumber
	out.StreamEstimatedLength = p.estimatedSize
	out.StreamFinalPacket = p.outcomeToString()
	if tx != "" {
		out.TransactionUUID = tx
	}
	out.Payload = buf

	p.currentPacketNumber++

	p.compress(&out)

	return &out
}

// outcomeToString converts the current outcome error into its string
// representation for inclusion in the WRP message.  If there is no outcome, an
// empty string is returned.
func (p *Packetizer) outcomeToString() string {
	switch {
	case p.outcome == nil:
		return ""
	case errors.Is(p.outcome, io.EOF):
		return "EOF"
	default:
		return p.outcome.Error()
	}
}

// compress applies the configured encoding to the message payload if it can.
// If encoding fails, the Packetizer falls back to identity encoding for this
// and all subsequent packets to avoid repeated compression failures.
func (p *Packetizer) compress(msg *simpleStreamingMessage) {
	if msg == nil || len(msg.Payload) == 0 {
		return
	}

	// Skip compression if already using identity encoding
	if p.encoding == EncodingIdentity {
		msg.StreamEncoding = EncodingIdentity
		return
	}

	payload, err := p.encoding.encode(msg.Payload)
	if err != nil {
		// On encoding error, fall back to identity encoding for all subsequent packets
		p.encoding = EncodingIdentity
		msg.StreamEncoding = EncodingIdentity
		return
	}

	msg.StreamEncoding = p.encoding
	msg.Payload = payload
}

func (p *Packetizer) readChunk(ctx context.Context) ([]byte, error) {
	buf := make([]byte, p.maxPacketSize)
	var got int
	var err error

	// Read until buffer full or error
	for err == nil && got < len(buf) {
		if ctx.Err() != nil {
			return buf[:got], ctx.Err()
		}

		var n int
		n, err = p.stream.Read(buf[got:])
		got += n

		// Detect buggy readers that return (0, nil) without making progress
		if n == 0 && err == nil {
			return buf[:got], io.ErrNoProgress
		}
	}
	return buf[:got], err
}
