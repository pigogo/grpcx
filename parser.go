// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pigogo/grpcx/codec"

	"github.com/pigogo/grpcx/compresser"
	xlog "github.com/pigogo/grpcx/grpclog"
)

const headMagic byte = 0x66
const headerLen = 8

func parseAndRecvMsg(reader *bufio.Reader, dc compresser.Decompressor, maxReceiveMessageSize int, rusedBuffer ...bool) (header *PackHeader, msg []byte, err error) {
	var (
		netHeader []byte
		rawHeader []byte
	)
	netHeader, err = reader.Peek(headerLen)
	if err != nil {
		return
	}

	if netHeader[0] != headMagic {
		err = fmt.Errorf("grpcx: received invalid message: invalid magic")
		xlog.Errorln(err)
		return
	}

	hlen := int(binary.BigEndian.Uint16(netHeader[2:4]))
	if hlen == 0 {
		err = fmt.Errorf("grpcx: received  invalid message: zero head len")
		xlog.Errorln(err)
		return
	}

	blen := int(binary.BigEndian.Uint32(netHeader[4:8]))
	if hlen+blen > maxReceiveMessageSize {
		err = fmt.Errorf("grpcx: received message larger than max (%d vs. %d)", blen, maxReceiveMessageSize)
		xlog.Errorln(err)
		return
	}

	err = checkRecvPayload(compresser.CompType(netHeader[1]), dc)
	if err != nil {
		xlog.Errorf("grpcx: checkRecvPayload fail:%v", err)
		return
	}

	reader.Discard(headerLen)
	rawHeader, err = reader.Peek(hlen)
	if err != nil {
		xlog.Errorf("grpcx: peek header fail:%v", err)
		return
	}

	//parse header
	header = new(PackHeader)
	err = header.Unmarshal(rawHeader)
	if err != nil {
		xlog.Errorf("grpcx: header unmarshal fail:%v", err)
		return
	}
	reader.Discard(hlen)

	doDecompress := bool(dc != nil && compresser.CompType(netHeader[1]) != compresser.CompTypeNone)
	// get msg body: if reused the buffer
	if doDecompress || rusedBuffer != nil {
		msg, err = reader.Peek(blen)
		reader.Discard(blen)
	} else {
		msg = make([]byte, blen)
		_, err = io.ReadFull(reader, msg)
	}

	// fail to get body
	if err != nil {
		xlog.Errorf("grpcx: peek body fail:%v", err)
		return
	}

	if doDecompress {
		if len(msg) > 0 {
			msg, err = dc.Do(bytes.NewReader(msg))
			if err != nil {
				xlog.Errorf("grpcx: body decode fail:%v", err)
				return
			}
		}
	}
	return
}

func encodeNetmsg(cd codec.Codec, cmp compresser.Compressor, header *PackHeader, msg interface{}, cbuf *bytes.Buffer, maxSendMsgSize int) ([]byte, error) {
	var (
		msgBuf []byte
	)

	hbuf, err := header.Marshal()
	if err != nil {
		xlog.Errorf("grpcx: header marshal fail:%v", err)
		return nil, err
	}

	if msg != nil {
		msgBuf, err = cd.Marshal(msg)
		if err != nil {
			xlog.Errorf("grpcx: body marshal fail:%v", err)
			return nil, err
		}
	}

	ct := compresser.CompTypeNone
	if cmp != nil && msg != nil {
		ct = cmp.Type()
	}

	head := [headerLen]byte{headMagic, byte(ct)}
	cbuf.Write(head[:])
	cbuf.Write(hbuf)

	hlen := len(hbuf)
	blen := len(msgBuf)
	if ct != compresser.CompTypeNone {
		if err = cmp.Do(cbuf, msgBuf); err != nil {
			xlog.Errorf("grpcx: body encompress fail:%v", err)
			return nil, err
		}
		blen = cbuf.Len() - headerLen - hlen
		if hlen+blen > maxSendMsgSize {
			xlog.Errorf("grpcx: send message size overflow:%v  max:%v", hlen+blen, maxSendMsgSize)
			return nil, fmt.Errorf("grpcx: message size overflow")
		}
	} else {
		if hlen+blen > maxSendMsgSize {
			xlog.Errorf("grpcx: send message size overflow:%v  max:%v", hlen+blen, maxSendMsgSize)
			return nil, fmt.Errorf("grpcx: message size overflow")
		}
		cbuf.Write(msgBuf)
	}

	out := cbuf.Bytes()
	binary.BigEndian.PutUint16(out[2:], uint16(hlen))
	binary.BigEndian.PutUint32(out[4:], uint32(blen))
	return out, nil
}
