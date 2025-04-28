// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/xmidt-org/wrp-go/v5"
	wrpssp "github.com/xmidt-org/wrpssp"
)

func Example() {
	sent := "This is an example of how to use the wrpssp package."

	packer, _ := wrpssp.New(
		wrpssp.ID("123"),
		wrpssp.Reader(strings.NewReader(sent)),
		// Split the string into 5 byte packets for the example or there would
		// only be one packet.  Normally this would be a much larger number.
		wrpssp.MaxPacketSize(5),

		// Normally this would be EncodingGzip, but for the example we are using
		// EncodingIdentity so that the packets are not compressed.
		wrpssp.WithEncoding(wrpssp.EncodingIdentity),
	)

	assembler := wrpssp.Assembler{}

	ctx := context.Background()
	for {
		// This is a pretty safe way to handle the packetizer.  If the packetizer
		// returns a msg, it should be sent.  The error might be interesting,
		// but it doesn't change what should be sent.
		msg, err := packer.Next(ctx, wrp.Message{
			Type:        wrp.SimpleEventMessageType,
			Source:      "self:",
			Destination: "event:foo",
		})
		if err != nil && !errors.Is(err, io.EOF) {
			panic(err)
		}
		if msg == nil {
			break
		}

		// Normally the message would have source, destination, and content type set
		// but for this example we are only interested in the payload, so there
		// is no need to set those fields in the msg.

		// Normally the msg would be sent out over the wire, but for this
		// example we are going to simply directly assemble the packets.
		err = assembler.ProcessWRP(context.Background(), *msg)
		if err != nil {
			break
		}
	}

	// The assembler will now have the original message.

	buf, _ := io.ReadAll(&assembler)

	fmt.Println(string(buf))

	// Output: This is an example of how to use the wrpssp package.
}

func Example_requestResponse() {
	sent := "This is an example of how to use the wrpssp package with a request/response."

	// This allows a predictable transaction ID to be used for the example.  Normally
	// this would be a random UUID.
	var tidCount int

	packer, _ := wrpssp.New(
		wrpssp.ID("123"),
		wrpssp.Reader(strings.NewReader(sent)),
		// Split the string into 5 byte packets for the example or there would
		// only be one packet.  Normally this would be a much larger number.
		wrpssp.MaxPacketSize(15),

		// Normally this would be EncodingGzip, but for the example we are using
		// EncodingIdentity so that the packets are not compressed.
		wrpssp.WithEncoding(wrpssp.EncodingIdentity),

		// Normally you'd want to use a normal uuid generator, but for the example
		// we are using a simple counter to generate a predictable transaction ID.
		wrpssp.WithUpdateTransactionUUID(func() (string, error) {
			defer func() {
				tidCount++
			}()
			return fmt.Sprintf("tid-%d", tidCount), nil
		}),
	)

	assembler := wrpssp.Assembler{}

	ctx := context.Background()
	for {
		// This is a pretty safe way to handle the packetizer.  If the packetizer
		// returns a msg, it should be sent.  The error might be interesting,
		// but it doesn't change what should be sent.
		msg, err := packer.Next(ctx, wrp.Message{
			Type:        wrp.SimpleRequestResponseMessageType,
			Source:      "self:",
			Destination: "event:foo",
			//TransactionUUID is automatically set by the packetizer.
		})

		if err != nil && !errors.Is(err, io.EOF) {
			panic(err)
		}
		if msg == nil {
			break
		}

		// Normally the message would have source, destination, and content type set
		// but for this example we are only interested in the payload, so there
		// is no need to set those fields in the msg.

		fmt.Printf("Transaction ID: %s\n", msg.TransactionUUID)

		// Normally the msg would be sent out over the wire, but for this
		// example we are going to simply directly assemble the packets.
		err = assembler.ProcessWRP(context.Background(), *msg)
		if err != nil {
			break
		}
	}

	// The assembler will now have the original message.

	buf, _ := io.ReadAll(&assembler)

	fmt.Println(string(buf))

	// Output:
	// Transaction ID: tid-0
	// Transaction ID: tid-1
	// Transaction ID: tid-2
	// Transaction ID: tid-3
	// Transaction ID: tid-4
	// Transaction ID: tid-5
	// Transaction ID: tid-6
	// This is an example of how to use the wrpssp package with a request/response.
}
