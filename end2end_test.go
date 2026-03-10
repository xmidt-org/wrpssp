// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xmidt-org/wrp-go/v5"
	wrpssp "github.com/xmidt-org/wrpssp/v2"
)

func TestEnd2End(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	data := generateLargeDataSet()

	p, err := wrpssp.New(wrpssp.ID("test"), wrpssp.Reader(bytes.NewReader(data)))
	require.NoError(err)
	assert.NotNil(p)

	dest := wrp.Message{
		Type:            wrp.SimpleEventMessageType,
		Source:          "self:",
		Destination:     "event:foo",
		TransactionUUID: "test",
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	assembler := &wrpssp.Assembler{}

	// Packatize and send the packets to the assembler.
	go func() {
		var err error
		for err == nil {
			var msg *wrp.Message
			msg, err = p.Next(ctx, dest)
			if msg != nil {
				// Process message even if there's an error (final packet has EOF in headers)
				_ = assembler.ProcessWRP(ctx, *msg)
			}
		}
		_ = assembler.Close()
	}()

	got, err := io.ReadAll(assembler)
	require.NoError(err)
	assert.Equal(data, got)
}

func generateLargeDataSet() []byte {
	data := make([]byte, 1024*1024)
	for i := 0; i < len(data); i++ {
		data[i] = byte(i % 256)
	}
	return data
}
