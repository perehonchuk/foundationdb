/*
 * ConflictBloomFilter.h
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

#ifndef CONFLICT_BLOOM_FILTER_H
#define CONFLICT_BLOOM_FILTER_H
#pragma once

#include <cstdint>
#include <vector>
#include "fdbclient/FDBTypes.h"

// Bloom filter for probabilistic conflict detection
// Used as a fast-path pre-check before the skip-list based conflict resolution
class ConflictBloomFilter {
public:
	ConflictBloomFilter(size_t bits, int numHashes);
	~ConflictBloomFilter();

	// Add a key range to the bloom filter
	void addRange(const StringRef& begin, const StringRef& end, Version version);

	// Check if a key range might conflict (may have false positives, no false negatives)
	bool mightConflict(const StringRef& begin, const StringRef& end) const;

	// Clear all entries at or before the given version
	void clearOldEntries(Version oldestVersion);

	// Reset the entire bloom filter
	void reset();

	// Get statistics
	size_t getSize() const { return bits.size(); }
	int getNumHashes() const { return numHashes; }

private:
	std::vector<bool> bits;
	int numHashes;
	std::map<Version, std::vector<std::pair<size_t, int>>> versionedEntries; // Track which bits belong to which version

	size_t hash(const StringRef& key, int hashIndex) const;
};

#endif
