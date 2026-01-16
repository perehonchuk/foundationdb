/*
 * ConflictBatchExtensions.cpp
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

#include "fdbserver/ConflictSet.h"
#include "fdbserver/ConflictBloomFilter.h"

// Bloom filter-based pre-check phase
// This quickly eliminates transactions that definitely don't conflict
// by checking against a probabilistic bloom filter of recent write ranges
void ConflictBatch::bloomFilterPrecheck(const ConflictBloomFilter* bloomFilter,
                                       std::vector<int>& potentialConflicts,
                                       std::vector<int>& noConflicts) {
	// Check each transaction's read ranges against the bloom filter
	for (int t = 0; t < transactionCount; t++) {
		bool mightConflict = false;

		// Check if any read conflict range overlaps with bloom filter entries
		if (transactionInfo[t]) {
			for (const auto& readRange : transactionInfo[t]->transaction.read_conflict_ranges) {
				if (bloomFilter->mightConflict(readRange.begin, readRange.end)) {
					mightConflict = true;
					break;
				}
			}
		}

		if (mightConflict) {
			potentialConflicts.push_back(t);
		} else {
			noConflicts.push_back(t);
		}
	}
}
