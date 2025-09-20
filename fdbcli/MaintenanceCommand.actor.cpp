/*
 * MaintenanceCommand.actor.cpp
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

#include <cinttypes>
#include <unordered_set>
#include <vector>

#include "boost/lexical_cast.hpp"
#include "fmt/format.h"

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/FDBTypes.h"
#include "fdbclient/IClientApi.h"

#include "fdbclient/Knobs.h"
#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

// print zone ids under maintenance; multiple zones can be active simultaneously
ACTOR Future<Void> printHealthyZone(Reference<IDatabase> db) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		try {
			// We need to keep the future as the returned standalone is not guaranteed to manage its memory when
			// using an external client, but the ThreadFuture holds a reference to the memory
			state ThreadFuture<RangeResult> resultFuture =
			    tr->getRange(fdb_cli::maintenanceSpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			RangeResult res = wait(safeThreadFutureToFuture(resultFuture));
			bool ignoreSSFailures = false;
			for (const auto& kv : res) {
				if (kv.key == fdb_cli::ignoreSSFailureSpecialKey) {
					ignoreSSFailures = true;
					break;
				}
			}

			if (ignoreSSFailures) {
				printf("Data distribution has been disabled for all storage server failures in this cluster and thus "
				       "maintenance mode is not active.\n");
				return Void();
			}

			std::vector<std::pair<std::string, int64_t>> zones;
			zones.reserve(res.size());
			for (const auto& kv : res) {
				std::string zoneId = kv.key.removePrefix(fdb_cli::maintenanceSpecialKeyRange.begin).toString();
				if (zoneId.empty()) {
					continue;
				}
				double secondsRemaining = 0;
				try {
					secondsRemaining = boost::lexical_cast<double>(kv.value.toString());
				} catch (const boost::bad_lexical_cast&) {
					continue;
				}
				if (secondsRemaining <= 0) {
					continue;
				}
				zones.emplace_back(std::move(zoneId), static_cast<int64_t>(secondsRemaining));
			}

			if (zones.empty()) {
				printf("No ongoing maintenance.\n");
			} else if (zones.size() == 1) {
				fmt::print("Maintenance for zone {0} will continue for {1} seconds.\n",
				           zones.front().first,
				           zones.front().second);
			} else {
				fmt::print("Maintenance for multiple zones:\n");
				for (const auto& zone : zones) {
					fmt::print("  - {0} ({1} seconds remaining)\n", zone.first, zone.second);
				}
			}
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

} // namespace

namespace fdb_cli {

const KeyRangeRef maintenanceSpecialKeyRange =
    KeyRangeRef("\xff\xff/management/maintenance/"_sr, "\xff\xff/management/maintenance0"_sr);
// The special key, if present, means data distribution is disabled for storage failures;
const KeyRef ignoreSSFailureSpecialKey = "\xff\xff/management/maintenance/IgnoreSSFailures"_sr;

// add zones to maintenance and specify the maintenance duration
ACTOR Future<bool> setHealthyZones(Reference<IDatabase> db,
	                                std::vector<std::string> zoneIds,
	                                double seconds,
	                                bool printWarning) {
	state std::vector<std::string> zoneList = std::move(zoneIds);
	state Reference<ITransaction> tr = db->createTransaction();
	TraceEvent("SetHealthyZone")
	    .detail("ZoneCount", static_cast<int>(zoneList.size()))
	    .detail("DurationSeconds", seconds);
	if (zoneList.empty()) {
		if (printWarning) {
			fprintf(stderr,
			        "ERROR: At least one zone must be specified when enabling maintenance mode.\n");
		}
		return false;
	}
	loop {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		try {
			// hold the returned standalone object's memory
			state ThreadFuture<RangeResult> resultFuture =
			    tr->getRange(fdb_cli::maintenanceSpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			RangeResult res = wait(safeThreadFutureToFuture(resultFuture));
			for (const auto& kv : res) {
				if (kv.key == fdb_cli::ignoreSSFailureSpecialKey) {
					if (printWarning) {
						fprintf(stderr,
						        "ERROR: Maintenance mode cannot be used while data distribution is disabled for storage "
						        "server failures. Use 'datadistribution on' to reenable data distribution.\n");
					}
					return false;
				}
			}

			tr->clear(fdb_cli::maintenanceSpecialKeyRange);
			std::string secondsStr = boost::lexical_cast<std::string>(seconds);
			for (const auto& zone : zoneList) {
				StringRef zoneRef(reinterpret_cast<const uint8_t*>(zone.data()), zone.size());
				tr->set(fdb_cli::maintenanceSpecialKeyRange.begin.withSuffix(zoneRef), secondsStr);
			}
			wait(safeThreadFutureToFuture(tr->commit()));
			return true;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR Future<bool> setHealthyZone(Reference<IDatabase> db, StringRef zoneId, double seconds, bool printWarning) {
	std::vector<std::string> zoneList;
	zoneList.push_back(zoneId.toString());
	bool success = wait(setHealthyZones(db, std::move(zoneList), seconds, printWarning));
	return success;
}

// clear ongoing maintenance, let clearSSFailureZoneString = true to enable data distribution for storage
ACTOR Future<bool> clearHealthyZone(Reference<IDatabase> db, bool printWarning, bool clearSSFailureZoneString) {
	state Reference<ITransaction> tr = db->createTransaction();
	TraceEvent("ClearHealthyZone").detail("ClearSSFailureZoneString", clearSSFailureZoneString);
	loop {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		try {
			// hold the returned standalone object's memory
			state ThreadFuture<RangeResult> resultFuture =
			    tr->getRange(fdb_cli::maintenanceSpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			RangeResult res = wait(safeThreadFutureToFuture(resultFuture));
			bool ignoreSSFailures = false;
			for (const auto& kv : res) {
				if (kv.key == fdb_cli::ignoreSSFailureSpecialKey) {
					ignoreSSFailures = true;
					break;
				}
			}
			if (!clearSSFailureZoneString && ignoreSSFailures) {
				if (printWarning) {
					fprintf(stderr,
					        "ERROR: Maintenance mode cannot be used while data distribution is disabled for storage "
					        "server failures. Use 'datadistribution on' to reenable data distribution.\n");
				}
				return false;
			}

			tr->clear(fdb_cli::maintenanceSpecialKeyRange);
			wait(safeThreadFutureToFuture(tr->commit()));
			return true;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR Future<bool> maintenanceCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	state bool result = true;
	if (tokens.size() == 1) {
		wait(printHealthyZone(db));
	} else if (tokens.size() == 2 && tokencmp(tokens[1], "off")) {
		bool clearResult = wait(clearHealthyZone(db, true));
		result = clearResult;
	} else if (tokens.size() >= 4 && tokencmp(tokens[1], "on")) {
		double seconds;
		int n = 0;
		auto secondsStr = tokens.back().toString();
		if (sscanf(secondsStr.c_str(), "%lf%n", &seconds, &n) != 1 || n != secondsStr.size()) {
			printUsage(tokens[0]);
			result = false;
		} else {
			std::unordered_set<std::string> seen;
			std::vector<std::string> zoneIds;
			for (int i = 2; i < tokens.size() - 1; ++i) {
				std::string rawToken = tokens[i].toString();
				size_t start = 0;
				while (start <= rawToken.size()) {
					size_t delim = rawToken.find(',', start);
					std::string zone = rawToken.substr(start, delim == std::string::npos ? std::string::npos : delim - start);
					if (!zone.empty() && seen.insert(zone).second) {
						zoneIds.push_back(zone);
					}
					if (delim == std::string::npos) {
						break;
					}
					start = delim + 1;
				}
			}

			if (zoneIds.empty()) {
				printUsage(tokens[0]);
				result = false;
			} else {
				bool setResult = wait(setHealthyZones(db, zoneIds, seconds, true));
				result = setResult;
			}
		}
	} else {
		printUsage(tokens[0]);
		result = false;
	}
	return result;
}

CommandFactory maintenanceFactory(
    "maintenance",
    CommandHelp(
        "maintenance [on|off] [ZONEID ...] [SECONDS]",
        "mark one or more zones for maintenance",
        "Calling this command with `on' prevents data distribution from moving data away from the processes with the "
        "specified ZONEID values. Data distribution will automatically be turned back on for the listed zones after the "
        "specified SECONDS have elapsed, or after a storage server with a different ZONEID fails. Calling this command "
        "with no arguments will display any ongoing maintenance. Calling this command with `off' will disable "
        "maintenance.\n"));
} // namespace fdb_cli
