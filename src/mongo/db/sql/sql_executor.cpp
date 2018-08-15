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

#include "mongo/db/operation_context.h"
#include "mongo/db/sql/sql_executor.h"
#include "mongo/db/sql/sql_impl.h"

namespace mongo {

void SqlDummyExecutor::execute(OperationContext* opCtx, SqlReplySender* replySender) {
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

std::unique_ptr<SqlExecutor> makeSqlExecutor(const std::string& dbName, const std::string& sql) {
    return std::make_unique<SqlDummyExecutor>();
}
}