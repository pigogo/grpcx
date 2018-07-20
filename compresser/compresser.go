/*
 *
 * Copyright 2014 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package compresser

import (
	"compress/gzip"
	"io"
	"io/ioutil"
	"sync"
)

type CompType byte

const (
	CompTypeNone CompType = iota
	CompTypeGzip 
)

// Compressor defines the interface gRPC uses to compress a message.
type Compressor interface {
	// Do compresses p into w.
	Do(w io.Writer, p []byte) (error)
	// Type returns the compression algorithm the Compressor uses.
	Type() CompType
}

type gzipCompressor struct {
	pool sync.Pool
}

// NewGZIPCompressor creates a Compressor based on GZIP.
func NewGZIPCompressor() Compressor {
	return &gzipCompressor{
		pool: sync.Pool{
			New: func() interface{} {
				return gzip.NewWriter(ioutil.Discard)
			},
		},
	}
}

func (c *gzipCompressor) Do(w io.Writer, p []byte)  error {
	z := c.pool.Get().(*gzip.Writer)
	z.Reset(w)
	_, err := z.Write(p)
	if err == nil {
		return z.Close()
	}
	return err
}

func (c *gzipCompressor) Type() CompType {
	return CompTypeGzip
}

// Decompressor defines the interface gRPC uses to decompress a message.
type Decompressor interface {
	// Do reads the data from r and uncompress them.
	Do(r io.Reader) ([]byte, error)
	// Type returns the compression algorithm the Decompressor uses.
	Type() CompType
}

type gzipDecompressor struct {
	pool sync.Pool
}

// NewGZIPDecompressor creates a Decompressor based on GZIP.
func NewGZIPDecompressor() Decompressor {
	return &gzipDecompressor{}
}

func (d *gzipDecompressor) Do(r io.Reader) ([]byte, error) {
	var z *gzip.Reader
	switch maybeZ := d.pool.Get().(type) {
	case nil:
		newZ, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		z = newZ
	case *gzip.Reader:
		z = maybeZ
		if err := z.Reset(r); err != nil {
			d.pool.Put(z)
			return nil, err
		}
	}

	defer func() {
		z.Close()
		d.pool.Put(z)
	}()
	return ioutil.ReadAll(z)
}

func (d *gzipDecompressor) Type() CompType {
	return CompTypeGzip
}
