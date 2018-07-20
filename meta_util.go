// Copyright (C) 2018 Kun Zhong All rights reserved.
// Use of this source code is governed by a Licensed under the Apache License, Version 2.0 (the "License");

package grpcx

import (
	"encoding/binary"

	"golang.org/x/net/context"
)

// metaKey metadata key type
type metaKeyType int32

const (
	tokenIDMetaKey metaKeyType = 1
)

type ctxMetaKey struct{}

// GetMetaFromContext used to get metadata from context
func GetMetaFromContext(ctx context.Context) map[int32][]byte {
	val := ctx.Value(ctxMetaKey{})
	if mt, ok := val.(map[int32][]byte); ok {
		return mt
	}
	return nil
}

func withMetadata(ctx context.Context, meta map[int32][]byte) context.Context {
	return context.WithValue(ctx, ctxMetaKey{}, meta)
}

func getServiceToken(meta map[int32][]byte) int64 {
	if val, ok := meta[int32(tokenIDMetaKey)]; ok && len(val) == 8 {
		return int64(binary.BigEndian.Uint64(val))
	}
	return 0
}

func withToken(meta map[int32][]byte, tid int64) map[int32][]byte {
	if meta == nil {
		meta = make(map[int32][]byte)
	}
	token := [8]byte{}
	binary.BigEndian.PutUint64(token[:], uint64(tid))
	meta[int32(tokenIDMetaKey)] = token[:]
	return meta
}
