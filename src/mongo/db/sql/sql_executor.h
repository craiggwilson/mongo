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

#include "mongo/db/jsobj.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/sql/sql_impl.h"

namespace mongo {

/**
 * SqlExecutor is responsible for sending replies. There are specialized implementations
 * of SqlExecutor to handle the different types of executions required.
 */
class SqlExecutor {
public:
    /**
     * execute performs the execution using the replySender to output its results.
     */ 
    virtual void execute(SqlReplySender* replySender) = 0;
};

/**
 * SqlDummyExecutor is a stub implemenation that returns dummy data.
 */ 
class SqlDummyExecutor final : public SqlExecutor {
public:
    void execute(SqlReplySender* replySender);
};

/**
 * SqlInsertExecutor inserts the data provided in the constructor to the specified database and collection.
 */ 
class SqlInsertExecutor final : public SqlExecutor {
public:
    SqlInsertExecutor(const std::string& databaseName, const std::string& collectionName, BSONObj obj);

    void execute(SqlReplySender* replySender);
private:
    BSONObj _obj;
};

/**
 * makeSqlExecutor creates an executor.
 */
std::unique_ptr<SqlExecutor> makeSqlExecutor(
    OperationContext* opCtx,
    const std::string& databaseName,
    const std::string& sql
);
}