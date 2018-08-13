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

#include <string>

#include "mongo/db/commands.h"
#include "mongo/db/sql/sql_impl.h"

namespace mongo {
class SQLCmd final : public Command {
public:
    SQLCmd() : Command("sql") {}

    std::unique_ptr<CommandInvocation> parse(OperationContext* opCtx,
                                             const OpMsgRequest& opMsgRequest) override {
        uassert(70001,
                "sql command requires a string argument named 'sql'",
                opMsgRequest.body["sql"].type() == String);
        return std::make_unique<Invocation>(
            this, opMsgRequest.body["sql"].checkAndGetStringData(), opMsgRequest.getDatabase());
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    class Invocation final : public CommandInvocation {
    public:
        Invocation(const SQLCmd* definition, StringData sql, StringData dbName)
            : CommandInvocation(definition), _sql(sql.toString()), _dbName(dbName.toString()) {}

        NamespaceString ns() const override {
            return NamespaceString(_dbName);
        }

        bool supportsWriteConcern() const override {
            return false;
        }

        void doCheckAuthorization(OperationContext* opCtx) const override {
            // TODO
        }

        void run(OperationContext* opCtx, rpc::ReplyBuilderInterface* result) override {
            const auto objs = runSQL(opCtx, _dbName, _sql);
            result->getBodyBuilder().append("rows", objs);
        }

    private:
        std::string _sql;
        std::string _dbName;
    };
} sqlCmd;
}
