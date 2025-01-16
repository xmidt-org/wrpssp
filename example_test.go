// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp_test

import (
	"context"
	"fmt"
	"io"
	"strings"

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
	)

	assembler := wrpssp.Assembler{}

	ctx := context.Background()
	for {
		// This is a pretty safe way to handle the packetizer.  If the packetizer
		// returns a msg, it should be sent.  The error might be interesting,
		// but it doesn't change what should be sent.
		msg, _ := packer.Next(ctx)

		// Normally the message would have source, destination, and content type set
		// but for this example we are only interested in the payload, so there
		// is no need to set those fields in the msg.

		// Normally the msg would be sent out over the wire, but for this
		// example we are going to simply directly assemble the packets.
		err := assembler.ProcessWRP(context.Background(), msg)
		if err != nil {
			break
		}
	}

	// The assembler will now have the original message.

	buf, _ := io.ReadAll(&assembler)

	fmt.Println(string(buf))

	// Output: This is an example of how to use the wrpssp package.
}
