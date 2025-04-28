// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpssp

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncoding_IsValid(t *testing.T) {
	tests := []struct {
		name     string
		encoding Encoding
		expected bool
	}{
		{"Valid Identity", EncodingIdentity, true},
		{"Valid Gzip", EncodingGzip, true},
		{"Valid Deflate", EncodingDeflate, true},
		{"Invalid Encoding", Encoding("invalid"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.encoding.isValid())
		})
	}
}

func TestEncoding_Is(t *testing.T) {
	tests := []struct {
		name     string
		encoding Encoding
		want     Encoding
		expected bool
	}{
		{"Identity Match", EncodingIdentity, EncodingIdentity, true},
		{"Gzip Match", EncodingGzip, EncodingGzip, true},
		{"Deflate Match", EncodingDeflate, EncodingDeflate, true},
		{"Identity Mismatch", EncodingGzip, EncodingIdentity, false},
		{"Invalid Encoding", Encoding("invalid"), EncodingGzip, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.encoding.is(tt.want))
		})
	}
}

func TestEncoding_String(t *testing.T) {
	tests := []struct {
		name     string
		encoding Encoding
		expected string
	}{
		{"Identity", EncodingIdentity, "identity"},
		{"Gzip", EncodingGzip, "gzip"},
		{"Gzip with Compression", EncodingGzipBestCompression, "gzip"},
		{"Deflate", EncodingDeflate, "deflate"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.encoding.string())
		})
	}
}

func TestEncoding_Encode(t *testing.T) {
	tests := []struct {
		name     string
		encoding Encoding
		data     []byte
		expected []byte
		wantErr  error
	}{
		{
			name:     "Identity Encoding",
			encoding: EncodingIdentity,
			data:     []byte("test data"),
			expected: []byte("test data"),
			wantErr:  nil,
		},
		{
			name:     "Gzip Encoding",
			encoding: EncodingGzip,
			data:     []byte("test data"),
			expected: func() []byte {
				var buf bytes.Buffer
				writer := gzip.NewWriter(&buf)
				_, err := writer.Write([]byte("test data"))
				if err != nil {
					panic(err)
				}
				if err = writer.Close(); err != nil {
					panic(err)
				}
				return buf.Bytes()
			}(),
			wantErr: nil,
		},
		{
			name:     "Deflate Encoding",
			encoding: EncodingDeflate,
			data:     []byte("test data"),
			expected: func() []byte {
				var buf bytes.Buffer
				writer, _ := flate.NewWriter(&buf, flate.DefaultCompression)
				_, err := writer.Write([]byte("test data"))
				if err != nil {
					panic(err)
				}
				if err = writer.Close(); err != nil {
					panic(err)
				}
				return buf.Bytes()
			}(),
			wantErr: nil,
		},
		{
			name:     "Unsupported Encoding",
			encoding: Encoding("unsupported"),
			data:     []byte("test data"),
			expected: nil,
			wantErr:  ErrUnsupportedEncoding,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.encoding.encode(tt.data)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestEncoding_Decode(t *testing.T) {
	tests := []struct {
		name     string
		encoding Encoding
		data     []byte
		expected []byte
		wantErr  error
	}{
		{
			name:     "Identity Decoding",
			encoding: EncodingIdentity,
			data:     []byte("test data"),
			expected: []byte("test data"),
			wantErr:  nil,
		},
		{
			name:     "Gzip Decoding",
			encoding: EncodingGzip,
			data: func() []byte {
				var buf bytes.Buffer
				writer := gzip.NewWriter(&buf)
				_, err := writer.Write([]byte("test data"))
				require.NoError(t, err)
				require.NoError(t, writer.Close())
				return buf.Bytes()
			}(),
			expected: []byte("test data"),
			wantErr:  nil,
		},
		{
			name:     "Deflate Decoding",
			encoding: EncodingDeflate,
			data: func() []byte {
				var buf bytes.Buffer
				writer, _ := flate.NewWriter(&buf, flate.DefaultCompression)
				_, err := writer.Write([]byte("test data"))
				require.NoError(t, err)
				require.NoError(t, writer.Close())
				return buf.Bytes()
			}(),
			expected: []byte("test data"),
			wantErr:  nil,
		},
		{
			name:     "Unsupported Encoding",
			encoding: Encoding("unsupported"),
			data:     []byte("test data"),
			expected: nil,
			wantErr:  ErrUnsupportedEncoding,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.encoding.decode(tt.data)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestEncoding_EncodeDecode(t *testing.T) {
	tests := []struct {
		name     string
		encoding Encoding
		data     []byte
	}{
		{
			name:     "Identity Encoding",
			encoding: EncodingIdentity,
			data:     []byte("test data"),
		},
		{
			name:     "Gzip Encoding",
			encoding: EncodingGzip,
			data:     []byte("test data"),
		},
		{
			name:     "Deflate Encoding",
			encoding: EncodingDeflate,
			data:     []byte("test data"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encodedData, err := tt.encoding.encode(tt.data)
			assert.NoError(t, err)

			decodedData, err := tt.encoding.decode(encodedData)
			assert.NoError(t, err)

			assert.Equal(t, tt.data, decodedData)
		})
	}
}
