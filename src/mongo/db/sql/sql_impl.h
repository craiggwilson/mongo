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

#include <string>
#include <vector>

#include "mongo/db/jsobj.h"
#include "mongo/db/operation_context.h"

namespace mongo {

using PgOid = int32_t;

enum class PgType : PgOid {
    // For now we only support returning Strings. Other types can be added later.
    kText = 25,
};

struct SqlColumnDesc {
    std::string name;  // For now only fill this in.

    PgOid sourceTable = 0;
    int16_t sourceColumn = 0;

    PgType type = PgType::kText;
    int16_t typeSize = -1;
    int32_t typeMod = 0;

    // Unrelated to PgType::kText. This is about the wire encoding. 0 = text, 1 = binary.
    int16_t textOrBinaryFormat = 0;
};

/**
 * Send replies to the client. Calls should follow one of these flows for each statement in the
 * request.
 *
 * Flow A: (For statements that return rows)
 *  - One call to sendRowDesc()
 *  - Zero or more calls to sendDataRow()
 *  - One call to sendCommandComplete()
 *
 * Flow B: (For statements that don't return rows)
 *  - One call to sendCommandComplete()
 *
 * Flow C: (Only for the empty statement)
 *  - One call to sendEmptyQueryResponse()
 */
class SqlReplySender {
public:
    virtual void sendRowDesc(const std::vector<SqlColumnDesc>& colls) = 0;

    /**
     * For now this only supports returning text-format, not binary format.
     */
    virtual void sendDataRow(const std::vector<boost::optional<std::string>>& colls) = 0;

    /**
     * msg should start with the "Command Tag" which identifies the command that is running.
     *
     * For SELECT operations it should be "SELECT " + count of rows returned.
     */
    virtual void sendCommandComplete(StringData msg) = 0;

    /**
     * Call this when the request is empty.
     */
    virtual void sendEmptyQueryResponse() = 0;

    /**
     * Returns the number of calls to sendDataRow() since the last call to sendRowDesc().
     * Only valid to call between calling sendRowDesc() and sendCommandComplete().
     */
    virtual int64_t nRowsSent() const = 0;

protected:
    SqlReplySender() = default;
    ~SqlReplySender() = default;  // Never destroyed polymorphically.

    SqlReplySender(SqlReplySender&&) = delete;
};

/**
 * This is used by the "sql" command.
 * It will probably go away soon.
 */
std::vector<BSONObj> runSQL(OperationContext* opCtx,
                            const std::string& dbName,
                            const std::string& sql);
}
