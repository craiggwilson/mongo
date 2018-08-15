/**
 *    Copyright (C) 2018 MongoDB Inc.
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
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include <pg_query.h>

#include "mongo/db/json.h"
#include "mongo/db/sql/sql_executor.h"
#include "mongo/db/sql/sql_impl.h"
#include "mongo/util/scopeguard.h"

// This is gross, but they aren't doing it in their headers so we need to include them like this.
// Also they are really macro heavy so this needs to be included *after* all of our headers.
extern "C" {
#include <postgres.h>  // This needs to be first.

#include <nodes/parsenodes.h>
#include <pg_query_internal.h>
#include <pg_query_json.h>
}

namespace mongo {

const auto reserveErrorCodes = ErrorCodes::Error(70000);  // reserve error codes >= 70000

/**
 * An RAII object that enters and exits a pg_query MemoryContext while in scope.
 */
class PgScopedMemoryContext {
public:
    explicit PgScopedMemoryContext(const char* name)
        : _memCtx(pg_query_enter_memory_context(name)) {}

    ~PgScopedMemoryContext() {
        pg_query_exit_memory_context(_memCtx);
    }

    PgScopedMemoryContext(PgScopedMemoryContext&&) = delete;
    PgScopedMemoryContext& operator=(PgScopedMemoryContext&&) = delete;

private:
    MemoryContext _memCtx;
};

void runSQL2(OperationContext* opCtx,
             const std::string& dbName,
             const std::string& sql,
             SqlReplySender* replySender) {
    
    SqlDummyExecutor executor;
    executor.execute(opCtx, replySender);
}

std::vector<BSONObj> runSQL(OperationContext* opCtx,
                            const std::string& dbName,
                            const std::string& sql) {
    std::vector<BSONObj> rows;

    auto builder = BSONObjBuilder(BSON("db" << dbName << "query" << sql));

    PgQueryParseResult result = pg_query_parse(sql.c_str());
    ON_BLOCK_EXIT([&] { pg_query_free_parse_result(result); });  // TODO wrap this in a type dtor.

    uassert(70002,
            str::stream() << "Error parsing SQL at " << result.error->cursorpos << ": "
                          << result.error->message,
            !result.error);

    builder.append("parsed", result.parse_tree);
    builder.append("parsed_tree", BSONArray(fromjson(result.parse_tree)));

    {
        PgScopedMemoryContext memCtx("mongo_sql_prarsing");
        PgQueryInternalParsetreeAndError rawResult = pg_query_raw_parse(sql.c_str());
        ON_BLOCK_EXIT([&] { free(rawResult.stderr_buffer); });  // TODO wrap this in a type dtor.
        if (rawResult.error) {
            ON_BLOCK_EXIT(
                [&] { pg_query_free_error(rawResult.error); });  // TODO wrap this in a type dtor.

            uasserted(70003,
                      str::stream() << "Error raw parsing SQL at " << rawResult.error->cursorpos
                                    << ": "
                                    << rawResult.error->message);
        }

        BSONArrayBuilder rawBuilder(builder.subarrayStart("raw_tree"));
        for (ListCell* stmtCell = list_head(rawResult.tree); stmtCell; stmtCell = lnext(stmtCell)) {
            invariant(IsA(stmtCell->data.ptr_value, RawStmt));
            auto rawStmt = castNode(RawStmt, stmtCell->data.ptr_value);
            if (IsA(rawStmt->stmt, SelectStmt)) {
                auto stmt = castNode(SelectStmt, rawStmt->stmt);
                BSONObjBuilder selectBuilder(rawBuilder.subobjStart());
                selectBuilder.append("kind", "SELECT");
                selectBuilder.append("from", pg_query_nodes_to_json(stmt->fromClause));
                selectBuilder.append("where", pg_query_nodes_to_json(stmt->whereClause));
                selectBuilder.append("targets", pg_query_nodes_to_json(stmt->targetList));
                // Just doing a few to get you started.
            }
        }
    }


    rows.push_back(builder.obj());

    return rows;
}
}
