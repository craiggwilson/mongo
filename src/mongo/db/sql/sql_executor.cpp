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

#include <string>
#include <vector>

#include "mongo/db/operation_context.h"
#include "mongo/db/sql/sql_executor.h"
#include "mongo/db/sql/sql_impl.h"
#include "mongo/db/jsobj.h"

#include <pg_query.h>

// This is gross, but they aren't doing it in their headers so we need to include them like this.
// Also they are really macro heavy so this needs to be included *after* all of our headers.
extern "C" {
#include <postgres.h>  // This needs to be first.

#include <nodes/parsenodes.h>
#include <pg_query_internal.h>
}

namespace mongo {

namespace {

class SqlCompositeExecutor final : public SqlExecutor {
public:
    SqlCompositeExecutor(const std::vector<SqlExecutor*>& executors)
        : _executors{std::move(executors)} {}

    void execute(SqlReplySender* replySender) override {
        for(auto const& it: _executors) {
            it->execute(replySender);
        }
    }

private:
    const std::vector<SqlExecutor*> _executors;
};

class SqlDummyExecutor final : public SqlExecutor {
public:
    void execute(SqlReplySender* replySender) override {
        // Dummy data
        const auto colls = std::vector<std::string>{"a", "b", "c"};
        const size_t nRows = 5;

        {
            // Prepare the header.
            auto rowDesc = std::vector<SqlColumnDesc>();
            for (auto&& collName : colls) {
                rowDesc.push_back(SqlColumnDesc{collName});
            }
            replySender->sendRowDesc(rowDesc);
        }


        for (size_t rowNum = 0; rowNum < nRows; rowNum++) {
            const size_t base = rowNum * colls.size();
            std::vector<boost::optional<std::string>> rowData;
            for (size_t collNum = 0; collNum < colls.size(); collNum++) {
                const auto data = base + collNum;
                if (data % 5 == 0) {
                    // Simulate a null.
                    rowData.emplace_back(boost::none);
                    continue;
                }

                rowData.emplace_back(std::to_string(data));
            }

            replySender->sendDataRow(rowData);
        }

        replySender->sendCommandComplete(str::stream() << "SELECT " << replySender->nRowsSent());
    }
};

class SqlInsertExecutor final : public SqlExecutor {
public:
    SqlInsertExecutor(std::string databaseName, std::string collectionName, BSONObj obj) {
        _obj = obj;
    }

    void execute(SqlReplySender* replySender) override {
        replySender->sendEmptyQueryResponse();
    }
private:
    BSONObj _obj;
};

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
}

std::unique_ptr<SqlExecutor> makeSqlExecutor(OperationContext* opCtx, const std::string& databaseName, const std::string& sql) {
    PgScopedMemoryContext memCtx("mongo_sql_parsing");
    PgQueryInternalParsetreeAndError rawResult = pg_query_raw_parse(sql.c_str());
    ON_BLOCK_EXIT([&] { free(rawResult.stderr_buffer); });  // TODO wrap this in a type dtor.
    if (rawResult.error) {
        ON_BLOCK_EXIT(
            [&] { pg_query_free_error(rawResult.error); });  // TODO wrap this in a type dtor.

        uasserted(70010,
                    str::stream() << "Error raw parsing SQL at " << rawResult.error->cursorpos
                                << ": "
                                << rawResult.error->message);
    }

    //TODO this should probably be vector<std:unique_ptr<SqlExecutor>>, but I can't make that work...
    std::vector<SqlExecutor*> executors;
    for (ListCell* stmtCell = list_head(rawResult.tree); stmtCell; stmtCell = lnext(stmtCell)) {
        invariant(IsA(stmtCell->data.ptr_value, RawStmt));
        auto rawStmt = castNode(RawStmt, stmtCell->data.ptr_value);

        switch(rawStmt->stmt->type) {
            case T_InsertStmt:
                executors.push_back(new SqlInsertExecutor(databaseName, "temp", BSON("a" << 1 << "b" << 1)));
                break;
            default:
                executors.push_back(new SqlDummyExecutor());
                break;
        }
    }
    
    return std::make_unique<SqlCompositeExecutor>(executors);
}
}
