// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"

	"github.com/xmidt-org/wrp-go/v3"
)

var (
	errNotHandled = errors.New("not handled")
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
	blocks  map[uint64]block
	m       sync.Mutex
}

var _ io.ReadCloser = (*Assembler)(nil)

type block struct {
	headers headers
	payload []byte
}

// Read implements an io.Reader method.
func (a *Assembler) Read(p []byte) (int, error) {
	a.m.Lock()
	defer a.m.Unlock()

	block, found := a.getBlock(a.current)
	if !found {
		err := a.getFinalState()
		if err != nil {
			a.close()
		}
		return 0, err
	}

	if block.headers.finalPacket != "" {
		a.final = strings.TrimSpace(block.headers.finalPacket)
	}

	n := copy(p, block.payload[a.offset:])
	a.offset += n

	if a.offset >= len(block.payload) {
		delete(a.blocks, a.current)

		a.current++
		a.offset = 0
	}

	err := a.getFinalState()
	if err != nil {
		a.close()
	}
	return n, err
}

func (a *Assembler) getBlock(n uint64) (block, bool) {
	if a.blocks == nil {
		return block{}, false
	}

	b, found := a.blocks[n]
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
	a.blocks = nil
	a.closed = true
}

// ProcessWRP takes a WRP message and processes it.  If the message is not an SSP
// message, it is ignored.  If the message is an SSP message, it is processed.
// The context is not used, but is required by the wrp.Processor interface.
func (a *Assembler) ProcessWRP(_ context.Context, msg wrp.Message) error {
	if !isSSP(&msg) {
		return errNotHandled
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

	if a.blocks == nil {
		a.blocks = make(map[uint64]block)
	}

	// We have the current packet already, so it can be dropped.
	if _, found := a.blocks[uint64(h.currentPacketNumber)]; found {
		return nil
	}

	a.blocks[uint64(h.currentPacketNumber)] = block{
		headers: h,
		payload: msg.Payload,
	}

	return nil
}
