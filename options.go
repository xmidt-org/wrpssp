// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import (
	"fmt"
	"io"
	"regexp"
)

// Option is a functional option for the Stream.
type Option interface {
	apply(*Packetizer) error
}

type optionFunc func(*Packetizer) error

func (f optionFunc) apply(file *Packetizer) error {
	return f(file)
}

// ID sets the ID of the stream.  The ID must be a non-empty string containing
// only [A-Za-z0-9 !#$&'()*+,./:;=?@[\]~_-].  This is a required field.
func ID(id string) Option {
	return optionFunc(func(s *Packetizer) error {
		s.id = id
		return nil
	})
}

// EstimatedLength sets the estimated length of the stream.  This is optional.
// If the size is less than 1, the default value of 0 is used.
//
// This field is used to help the receiver determine the progress of the stream
// if it is a fixed length.
func EstimatedLength(size int64) Option {
	return optionFunc(func(s *Packetizer) error {
		if size < 1 {
			size = 0
		}
		s.estimatedSize = uint64(size)
		return nil
	})
}

// Reader sets the stream to read from.  This is a required field.
func Reader(r io.Reader) Option {
	return optionFunc(func(s *Packetizer) error {
		s.stream = r
		return nil
	})
}

// MaxPacketSize sets the maximum size of a packet.  This is optional.  If the
// size is less than 1, the default value of 64KB is used.
func MaxPacketSize(size int) Option {
	return optionFunc(func(s *Packetizer) error {
		if size < 1 {
			size = 64 * 1024
		}
		s.maxPacketSize = size
		return nil
	})
}

// WithEncoding sets the encoding of the stream.  This is optional.  If the encoding
// is not set, the default value of EncodingGzip is used.
func WithEncoding(e Encoding) Option {
	return optionFunc(func(s *Packetizer) error {
		s.encoding = e
		return nil
	})
}

// WithUpdateTransactionUUID sets the function to generate a new transaction
// UUID for each packet.  This is optional.  If the function is not set, the
// default value of nil is used.
//
// This is useful for generating a new transaction UUID for each packet in the
// stream for a request/response protocol.  The function should return a new
// transaction UUID and an error.  The error should be nil if the function
// succeeds.
func WithUpdateTransactionUUID(fn func() (string, error)) Option {
	return optionFunc(func(s *Packetizer) error {
		s.txGen = fn
		return nil
	})
}

// validate ensures that the stream is valid before returning it.
func finalize() Option {
	return optionFunc(func(s *Packetizer) error {
		if s.stream == nil {
			return fmt.Errorf("%w: stream must not be nil", ErrInvalidInput)
		}

		if s.id == "" {
			return fmt.Errorf("%w: id must not be empty", ErrInvalidInput)
		}

		re := regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
		if !re.MatchString(s.id) {
			return fmt.Errorf("%w: id contains invalid characters", ErrInvalidInput)
		}

		if !s.encoding.isValid() {
			return fmt.Errorf("%w: encoding is invalid", ErrInvalidInput)
		}

		return nil
	})
}
