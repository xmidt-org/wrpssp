// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"errors"
	"io"
	"strings"
)

const (
	EncodingIdentity               Encoding = "identity"
	EncodingGzip                   Encoding = "gzip"
	EncodingGzipNoCompression      Encoding = "gzip+none"
	EncodingGzipBestSpeed          Encoding = "gzip+fastest"
	EncodingGzipBestCompression    Encoding = "gzip+best"
	EncodingGzipHuffmanOnly        Encoding = "gzip+huffman"
	EncodingDeflate                Encoding = "deflate"
	EncodingDeflateNoCompression   Encoding = "deflate+none"
	EncodingDeflateBestSpeed       Encoding = "deflate+fastest"
	EncodingDeflateBestCompression Encoding = "deflate+best"
	EncodingDeflateHuffmanOnly     Encoding = "deflate+huffman"
)

var (
	ErrUnsupportedEncoding = errors.New("unsupported encoding")
)

var compressionLevels = map[Encoding]int{
	EncodingGzip:                   gzip.DefaultCompression,
	EncodingGzipNoCompression:      gzip.NoCompression,
	EncodingGzipBestSpeed:          gzip.BestSpeed,
	EncodingGzipBestCompression:    gzip.BestCompression,
	EncodingGzipHuffmanOnly:        gzip.HuffmanOnly,
	EncodingDeflate:                gzip.DefaultCompression,
	EncodingDeflateNoCompression:   gzip.NoCompression,
	EncodingDeflateBestSpeed:       gzip.BestSpeed,
	EncodingDeflateBestCompression: gzip.BestCompression,
	EncodingDeflateHuffmanOnly:     gzip.HuffmanOnly,
}

type Encoding string

func (e Encoding) isValid() bool {
	switch e {
	case "", EncodingIdentity, EncodingGzip, EncodingDeflate:
		return true
	default:
		return false
	}
}

func (e Encoding) is(want Encoding) bool {
	if want == EncodingIdentity {
		return e == "" || e == EncodingIdentity
	}

	return e == want && e.isValid()
}

func (e Encoding) string() string {
	return strings.SplitN(string(e), "+", 2)[0]
}

func (e Encoding) encode(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	var writer io.WriteCloser
	var err error

	switch {
	case e.is(EncodingIdentity):
		return data, nil
	case strings.HasPrefix(string(e), "gzip"):
		writer, err = gzip.NewWriterLevel(&buf, compressionLevels[e])
	case strings.HasPrefix(string(e), "deflate"):
		writer, err = flate.NewWriter(&buf, compressionLevels[e])
	default:
		err = ErrUnsupportedEncoding
	}

	if err == nil {
		_, err = writer.Write(data)
		if err == nil {
			err = writer.Close()
			if err == nil {
				return buf.Bytes(), nil
			}
		}
	}

	return nil, err
}

func (e Encoding) decode(data []byte) ([]byte, error) {
	var reader io.Reader
	var err error

	switch {
	case e.is(EncodingIdentity):
		return data, nil
	case strings.HasPrefix(string(e), "gzip"):
		reader, err = gzip.NewReader(bytes.NewReader(data))
	case strings.HasPrefix(string(e), "deflate"):
		reader = flate.NewReader(bytes.NewReader(data))
	default:
		err = ErrUnsupportedEncoding
	}

	if err != nil {
		return nil, err
	}

	return io.ReadAll(reader)
}
