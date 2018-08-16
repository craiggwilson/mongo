/**
 *    Copyright 2018 MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#pragma once

#include <memory>
#include <string>

#include "mongo/db/operation_context.h"
#include "mongo/db/sql/sql_executor.h"

// This is gross, but they aren't doing it in their headers so we need to include them like this.
// Also they are really macro heavy so this needs to be included *after* all of our headers.
extern "C" {
#include <postgres.h>  // This needs to be first.

#include <nodes/parsenodes.h>
}

namespace mongo {

/**
 * SqlPlanner is responsible for create executors from a postgres RawStmt.
 */
class SqlPlanner {
public:
    ~SqlPlanner() = default;  // Never destroyed polymorphically.

    /**
     * plan takes a RawStmt and creates a plan for execution, ultimately returning a SqlExecutor.
     */ 
    virtual std::unique_ptr<SqlExecutor> plan(Node *node) = 0;

protected:
    SqlPlanner() = default;
    SqlPlanner(SqlPlanner&&) = delete;
};

/**
 * makeSqlPlanner creates a SqlPlanner.
 */ 
std::unique_ptr<SqlPlanner> makeSqlPlanner(OperationContext* opCtx, const std::string& databaseName);
}