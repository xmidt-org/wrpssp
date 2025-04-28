// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import (
	"context"
	"io"
	"strings"
	"sync"

	"github.com/xmidt-org/wrp-go/v5"
)

// Assembler is a struct that reads from a stream of WRP messages and assembles
// them into a single stream.
//
// The Assembler implements the io.ReadCloser interface, as well as the wrp.Processor
// interface.
type Assembler struct {
	Validators []wrp.Processor
	closed     bool
	current    int64
	final      string
	offset     int
	packets    map[int64]*simpleStreamingMessage
	m          sync.Mutex

	decoded *decoded
}

type decoded struct {
	number int64
	data   []byte
}

var _ io.ReadCloser = (*Assembler)(nil)

// Read implements an io.Reader method.
func (a *Assembler) Read(p []byte) (int, error) {
	a.m.Lock()
	defer a.m.Unlock()

	packet, buf, found, err := a.getPacket(a.current)
	if err != nil {
		// There was a decoding error, so the Assembler should be closed.
		a.final = err.Error()
		a.close()
		return 0, err
	}

	if !found {
		err := a.getFinalState()
		if err != nil {
			a.close()
		}
		return 0, err
	}

	if packet.StreamFinalPacket != "" {
		a.final = strings.TrimSpace(packet.StreamFinalPacket)
	}

	n := copy(p, buf[a.offset:])
	a.offset += n

	if a.offset >= len(buf) {
		delete(a.packets, a.current)

		a.current++
		a.decoded = nil
		a.offset = 0
	}

	err = a.getFinalState()
	if err != nil {
		a.close()
	}
	return n, err
}

func (a *Assembler) getPacket(n int64) (*simpleStreamingMessage, []byte, bool, error) {
	if a.packets == nil {
		return &simpleStreamingMessage{}, nil, false, nil
	}

	msg, found := a.packets[n]
	if !found {
		return nil, nil, false, nil
	}

	// We have the decoded packet already, so it can be returned.
	if a.decoded != nil && a.decoded.number == n {
		return msg, a.decoded.data, true, nil
	}

	if a.decoded == nil {
		a.decoded = &decoded{}
	}

	var err error
	a.decoded.data, err = msg.StreamEncoding.decode(msg.Payload)
	if err != nil {
		a.decoded = nil
		return nil, nil, true, err
	}
	a.decoded.number = n

	return msg, a.decoded.data, true, err
}

func (a *Assembler) getFinalState() error {
	if a.final == "" {
		return nil
	}

	if strings.ToLower(a.final) == "eof" {
		return io.EOF
	}

	return &unexpectedEOF{message: a.final}
}

// Close closes the Assembler and implements the io.Closer interface.
func (a *Assembler) Close() error {
	a.m.Lock()
	defer a.m.Unlock()

	a.close()

	return nil
}

func (a *Assembler) close() {
	a.packets = nil
	a.decoded = nil
	a.closed = true
}

// ProcessWRP takes a WRP message and processes it.  If the message is not an SSP
// message, it is ignored.  If the message is an SSP message, it is processed.
// The context is not used, but is required by the wrp.Processor interface.
func (a *Assembler) ProcessWRP(_ context.Context, msg wrp.Message) error {
	if !Is(&msg, a.Validators...) {
		return wrp.ErrNotHandled
	}

	var ssp simpleStreamingMessage
	if err := ssp.From(&msg, a.Validators...); err != nil {
		return err
	}

	a.m.Lock()
	defer a.m.Unlock()

	if a.closed {
		return ErrClosed
	}

	// We're past the current packet, so it can be dropped.
	if a.current > ssp.StreamPacketNumber {
		return nil
	}

	if a.packets == nil {
		a.packets = make(map[int64]*simpleStreamingMessage)
	}

	// We have the current packet already, so it can be dropped.
	if _, found := a.packets[ssp.StreamPacketNumber]; found {
		return nil
	}

	a.packets[ssp.StreamPacketNumber] = &ssp

	return nil
}
