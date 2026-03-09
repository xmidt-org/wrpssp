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
//
// Thread-safety: The Assembler is safe for concurrent use between one reader and
// multiple writers. A single goroutine should call Read(), while one or more
// goroutines may call ProcessWRP() to deliver packets. Multiple concurrent Read()
// calls are not supported and will result in undefined behavior.
type Assembler struct {
	Validators []wrp.Processor
	// Maximum allowed gap between current and received packet number (0 = unlimited)
	MaxPacketGap int

	closed  bool
	current int64
	final   error
	offset  int
	m       sync.Mutex
	decoded *decoded

	once    sync.Once
	packets map[int64]*simpleStreamingMessage
	event   chan struct{} // Signals when data arrives or close occurs
}

type decoded struct {
	number int64
	data   []byte
}

var _ io.ReadCloser = (*Assembler)(nil)

func (a *Assembler) init() {
	a.once.Do(func() {
		a.event = make(chan struct{}, 1)
		if a.packets == nil {
			a.packets = make(map[int64]*simpleStreamingMessage)
		}
	})
}

// Read implements an io.Reader method.
func (a *Assembler) Read(p []byte) (int, error) {
	a.init()

	var offset int
	for {
		a.m.Lock()
		n, err := a.read(p[offset:])
		a.m.Unlock()

		offset += n

		// Return if we hit an error or EOF
		if err != nil {
			return offset, err
		}

		// Got data - try to fill buffer if there's space
		if n > 0 && offset < len(p) {
			continue
		}

		// Buffer is full or we got some data
		if offset > 0 {
			return offset, nil
		}

		// Wait for more data
		<-a.event
	}
}

// read attempts to read from the current packet. Must be called with lock held.
// Returns data if available, or (0, nil) if we need to wait for more packets.
func (a *Assembler) read(p []byte) (int, error) {
	// Check if we've reached the end of the stream
	if done, err := a.checkStreamEnd(); done {
		return 0, err
	}

	// Try to get the current packet
	packet, buf, err := a.getPacket(a.current)
	if err != nil {
		// Decoding error - close and return
		a.final = err
		a.close()
		return 0, err
	}

	// Packet not ready yet
	if packet == nil {
		return 0, nil
	}

	// Process packet: copy data and handle completion
	n := a.processPacket(packet, buf, p)

	// Return data with final state error if present
	return n, a.final
}

// processPacket copies data from the packet buffer, updates state, and handles stream completion.
func (a *Assembler) processPacket(packet *simpleStreamingMessage, buf []byte, p []byte) int {
	// Capture final state marker and close if present
	if packet.StreamFinalPacket != "" {
		finalMsg := strings.TrimSpace(packet.StreamFinalPacket)
		if strings.ToLower(finalMsg) == "eof" {
			a.final = io.EOF
		} else {
			a.final = newUnexpectedEOF(finalMsg)
		}
		a.close()

		// Drop any packets after the final packet
		for num := range a.packets {
			if num > a.current {
				delete(a.packets, num)
			}
		}
	}

	// Copy available data
	n := copy(p, buf[a.offset:])
	a.offset += n

	// Advance to next packet if this one is fully consumed
	if a.offset >= len(buf) {
		delete(a.packets, a.current)
		a.current++
		a.decoded = nil
		a.offset = 0
	}

	return n
}

// checkStreamEnd determines if the stream has ended.
// Returns (true, error) if stream is complete, (false, nil) if more data may arrive.
func (a *Assembler) checkStreamEnd() (bool, error) {
	// Stream not closed yet, more data may arrive
	if !a.closed {
		return false, nil
	}

	// Current packet exists, we can continue reading
	if a.packets[a.current] != nil {
		return false, nil
	}

	// Stream is closed and current packet unavailable - we're done
	// Determine final state if not already set
	if a.final == nil {
		// Check if there are unreachable packets buffered (indicating a gap)
		if len(a.packets) > 0 {
			a.final = newUnexpectedEOF("missing packet")
		} else {
			a.final = io.EOF
		}
	}

	// Clear any unreachable buffered packets (due to gaps)
	clear(a.packets)
	a.decoded = nil

	if a.final != nil {
		return true, a.final
	}
	return true, io.EOF
}

// getPacket retrieves and decodes packet n if available.
// Returns (packet, decodedData, error).
// Returns (nil, nil, nil) if packet not available yet.
func (a *Assembler) getPacket(n int64) (*simpleStreamingMessage, []byte, error) {
	msg, found := a.packets[n]
	if !found {
		return nil, nil, nil
	}

	// Return cached decoded data if available
	if a.decoded != nil && a.decoded.number == n {
		return msg, a.decoded.data, nil
	}

	// Decode the packet
	if a.decoded == nil {
		a.decoded = &decoded{}
	}

	var err error
	a.decoded.data, err = msg.StreamEncoding.decode(msg.Payload)
	if err != nil {
		a.decoded = nil
		return nil, nil, err
	}
	a.decoded.number = n

	return msg, a.decoded.data, nil
}

// Close closes the Assembler and implements the io.Closer interface.
func (a *Assembler) Close() error {
	a.m.Lock()
	defer a.m.Unlock()

	a.close()

	return nil
}

func (a *Assembler) close() {
	if !a.closed {
		a.closed = true
		a.signal() // Wake up any blocked readers
	}
}

// ProcessWRP takes a WRP message and processes it.  If the message is not an SSP
// message, it is ignored.  If the message is an SSP message, it is processed.
func (a *Assembler) ProcessWRP(_ context.Context, msg wrp.Message) error {
	a.init()

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

	// We have the current packet already, so it can be dropped.
	if _, found := a.packets[ssp.StreamPacketNumber]; found {
		return nil
	}

	// Check if packet gap exceeds the limit
	if a.MaxPacketGap > 0 {
		gap := ssp.StreamPacketNumber - a.current
		if gap > int64(a.MaxPacketGap) {
			return &packetGapExceeded{
				current:  a.current,
				received: ssp.StreamPacketNumber,
				maxGap:   a.MaxPacketGap,
			}
		}
	}

	a.packets[ssp.StreamPacketNumber] = &ssp

	// Signal waiting readers that data is available
	a.signal()

	return nil
}

// signal notifies a waiting reader (must be called with lock held)
func (a *Assembler) signal() {
	if a.event != nil {
		select {
		case a.event <- struct{}{}:
		default:
			// Channel already has a pending signal, no need to add another
		}
	}
}
