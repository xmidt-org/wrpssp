# WRP Simple Streaming Protocol

## Abstract

WRP messages are similar to IP packets.  Each has a destination, a time to be
transmit across the wire and a size.  The size is limited by an unsigned 64-bit
number instead of an unsigned 16-bit number, but other limitations apply.  Yet,
As an operator of a Xmidt system, you may want to limit the size of a message
flowing through the system to reduce runtime memory at each hop for the WRP
packets.  Thus the need for a streaming and reassembly protocol.

## 1. Introduction

The WRP Simple Streaming Protocol (WRP-SSP) is designed to facilitate the
efficient transmission of large messages across a network by breaking them into
smaller, manageable packets.  This protocol is particularly useful in
environments where memory constraints and network reliability are critical
factors.  By leveraging a streaming and reassembly mechanism, WRP-SSP ensures
that large messages can be transmitted without overwhelming the system's resources.

The protocol defines a set of control headers that are used to manage the
streaming process, including identifiers for the stream, packet sequencing, and
optional metadata about the total length of the stream. These headers enable the
receiving system to correctly reassemble the packets into the original message,
ensuring data integrity and completeness.

WRP-SSP is designed to work bi-directionally, making it suitable for various use
cases such as file transfers from Customer Premises Equipment (CPE) or streaming
data from CPE devices. It also supports request-response handling through events,
providing a flexible and robust solution for different communication scenarios
within a Xmidt system.

## 2. Data Structures

To stream using the simple streaming protocol, the "Simple Event " type
(msg_type = 4) message type should be used.  This works bi-directionally.  This
is mainly designed for sending files from a CPE or streaming data from the CPE,
and request response handling will need to be handled via events instead of the
simpler "Simple Request-Response" (msg_type = 3) message type.

The destination (dst) field of the Simple Event message must be in the following format:
```bnf
event:<application-name>/<stream-id>

<application-name> ::= <string>
<stream-id> ::= <string>
```
- `application-id`: A unique application identifier.
- `stream-id`: The unique stream identifier.

The wrp message headers field should contain the following control headers:

```bnf
<stream-packet-number> ::= [1-9][0-9]*
<stream-final-packet> ::= <string>
<stream-estimated-total-length> ::= [1-9][0-9]*
```

Any whitespace found is ignored as well as the case of the labels.

- `stream-packet-number`: **Required** The 0-index based packet reassembly order.
- `stream-final-packet`: **Required** Marks the final packet in the stream and
   end of stream reason.  Only present in the final packet.
- `stream-estimated-total-length`: **Optional** Indicates the estimated total
   length if the stream is a known size.  The value is informative only.

### String Grammar
The following grammar is used for above fields defined as <string>:
```bnf
<string>  ::= <letter> | <digit> | <symbol> *
<letter>         ::= "A" | "B" | "C" | "D" | "E" | "F" | "G" | "H" | "I" | "J" |
                     "K" | "L" | "M" | "N" | "O" | "P" | "Q" | "R" | "S" | "T" |
                     "U" | "V" | "W" | "X" | "Y" | "Z" |
                     "a" | "b" | "c" | "d" | "e" | "f" | "g" | "h" | "i" | "j" |
                     "k" | "l" | "m" | "n" | "o" | "p" | "q" | "r" | "s" | "t" |
                     "u" | "v" | "w" | "x" | "y" | "z"
<digit>          ::= "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"
<symbol>         ::= " " | "!" | "#" | "$" | "&" | "'" | "(" | ")" | "*" | "+" |
                     "," | "-" | "." | "/" | ":" | ";" | "=" | "?" | "@" | "[" |
                     "\" | "]" | "_" | | "~"
```

## 3. Segmentation and Reassembly

Data should be segmented into smaller blocks and placed in the `payload` field
of the WRP message.  The additional `header` fields described in section 2 are
also required.

Reassembly involves placing the received WRP payload in the correct order within
the assembly buffer.

Packets MAY be delivered out of order and this MUST be tolerated.

## 4. Duplicate Packets

Packets MAY be duplicated by any part of the system.  Duplicate packets MUST
be tolerated, and ignored by the consumer.

Using duplicate packets is an easy way to increase the reliability of ensuring
all the parts of a message can make it to the destination.  This does increase
network bandwith costs, so it is advisable to use this with caution.

## 5. Limitations

This protocol is designed to be simple and work with the existing infrastructure
without modifications and thus has a few limitations.

### Lost Packets

Packet loss MAY happen.  It is outside the scope of the protocol to address how
to handle this.
