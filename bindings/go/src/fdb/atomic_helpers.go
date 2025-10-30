/*
 * atomic_helpers.go
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
 */

// FoundationDB Go API

package fdb

import "encoding/binary"

// AddInt64 performs an atomic addition of the provided delta encoded as a
// little-endian int64. This helper removes the need for callers to hand-roll
// the encoding required by Transaction.Add when building counters.
func (t Transaction) AddInt64(key KeyConvertible, delta int64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(delta))
	t.Add(key, buf[:])
}
