/*
 * ConflictBloomFilter.cpp
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

#include "fdbserver/ConflictBloomFilter.h"
#include <functional>

ConflictBloomFilter::ConflictBloomFilter(size_t numBits, int hashes)
  : bits(numBits, false), numHashes(hashes) {}

ConflictBloomFilter::~ConflictBloomFilter() {}

size_t ConflictBloomFilter::hash(const StringRef& key, int hashIndex) const {
	std::hash<std::string> hasher;
	std::string keyStr(reinterpret_cast<const char*>(key.begin()), key.size());
	size_t h = hasher(keyStr);
	// Mix in the hash index for different hash functions
	h ^= (hashIndex * 0x9e3779b9);
	return h % bits.size();
}

void ConflictBloomFilter::addRange(const StringRef& begin, const StringRef& end, Version version) {
	for (int i = 0; i < numHashes; i++) {
		size_t pos1 = hash(begin, i);
		size_t pos2 = hash(end, i);
		bits[pos1] = true;
		bits[pos2] = true;
		versionedEntries[version].push_back(std::make_pair(pos1, i));
		versionedEntries[version].push_back(std::make_pair(pos2, i));
	}
}

bool ConflictBloomFilter::mightConflict(const StringRef& begin, const StringRef& end) const {
	for (int i = 0; i < numHashes; i++) {
		size_t pos1 = hash(begin, i);
		size_t pos2 = hash(end, i);
		if (bits[pos1] || bits[pos2]) {
			return true;
		}
	}
	return false;
}

void ConflictBloomFilter::clearOldEntries(Version oldestVersion) {
	auto it = versionedEntries.begin();
	while (it != versionedEntries.end() && it->first < oldestVersion) {
		for (const auto& entry : it->second) {
			bits[entry.first] = false;
		}
		it = versionedEntries.erase(it);
	}
}

void ConflictBloomFilter::reset() {
	std::fill(bits.begin(), bits.end(), false);
	versionedEntries.clear();
}
