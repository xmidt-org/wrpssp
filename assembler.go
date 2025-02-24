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
	closed  bool
	current uint64
	final   string
	offset  int
	packets map[uint64]*SimpleStreamingMessage
	m       sync.Mutex
}

var _ io.ReadCloser = (*Assembler)(nil)

// Read implements an io.Reader method.
func (a *Assembler) Read(p []byte) (int, error) {
	a.m.Lock()
	defer a.m.Unlock()

	packet, found := a.getPacket(a.current)
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

	n := copy(p, packet.SimpleEvent.Payload[a.offset:])
	a.offset += n

	if a.offset >= len(packet.SimpleEvent.Payload) {
		delete(a.packets, a.current)

		a.current++
		a.offset = 0
	}

	err := a.getFinalState()
	if err != nil {
		a.close()
	}
	return n, err
}

func (a *Assembler) getPacket(n uint64) (*SimpleStreamingMessage, bool) {
	if a.packets == nil {
		return &SimpleStreamingMessage{}, false
	}

	b, found := a.packets[n]
	return b, found
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
	a.closed = true
}

// ProcessWRP takes a WRP message and processes it.  If the message is not an SSP
// message, it is ignored.  If the message is an SSP message, it is processed.
// The context is not used, but is required by the wrp.Processor interface.
func (a *Assembler) ProcessWRP(_ context.Context, msg wrp.Message) error {
	var ssp SimpleStreamingMessage
	if err := ssp.From(&msg); err != nil {
		return err
	}
	if !isSSP(&msg) {
		return wrp.ErrNotHandled
	}

	h, err := get(&msg)
	if err != nil {
		return err
	}

	a.m.Lock()
	defer a.m.Unlock()

	if a.closed {
		return ErrClosed
	}

	// We're past the current packet, so it can be dropped.
	if a.current > uint64(h.currentPacketNumber) {
		return nil
	}

	if a.packets == nil {
		a.packets = make(map[uint64]block)
	}

	// We have the current packet already, so it can be dropped.
	if _, found := a.packets[uint64(h.currentPacketNumber)]; found {
		return nil
	}

	a.packets[uint64(h.currentPacketNumber)] = block{
		headers: h,
		payload: msg.Payload,
	}

	return nil
}

// GetStreamID returns the stream ID of the message if it is an SSP message.
func GetStreamID(msg wrp.Message) (string, error) {
	if !isSSP(&msg) {
		return "", wrp.ErrNotHandled
	}

	h, err := get(&msg)
	if err != nil {
		return "", err
	}

	return h.id, nil
}
