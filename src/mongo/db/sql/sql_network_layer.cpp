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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kNetwork

#include "mongo/platform/basic.h"

#include "mongo/db/sql/sql_network_layer.h"

#include "mongo/db/sql/sql_impl.h"
#include "mongo/util/net/ssl_options.h"
#include "mongo/util/net/ssl_types.h"

#include <asio.hpp>

#include "mongo/db/client.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/transport/asio_utils.h"
#include "mongo/util/hex.h"
#include "mongo/util/log.h"

namespace mongo {

namespace {

struct SqlMessage {
    enum class Kind : uint8_t {
        kStartup = '\0',
        kAuth = 'R',
        kSimpleQuery = 'Q',
        kReadyForQuery = 'Z',
        kParamStatus = 'S',
        kKeyData = 'K',
        kRowDescription = 'T',
        kDataRow = 'D',
        kCommandComplete = 'C',
        kError = 'E',
        kEmptyQuery = 'I',
        kTerminate = 'X',
    };

    Kind kind() const {
        return Kind(message.get()[0]);
    }

    int32_t len() const {
        return ConstDataView(message.get() + 1).read<BigEndian<int32_t>>();
    }

    const char* data() const {
        return message.get() + 5;
    }

    auto buffer() {
        return asio::buffer(message.get(), len() + 1);
    }

    ConstSharedBuffer message;
};

class SqlMessageBuilder {
public:
    SqlMessageBuilder(SqlMessage::Kind kind) {
        MONGO_STATIC_ASSERT(sizeof(kind) == 1);
        _bb.appendBuf(&kind, 1);
        _bb.skip(4);  // size
    }

    SqlMessageBuilder& appendByte(uint8_t byte) {
        _bb.appendUChar(byte);
        return *this;
    }

    SqlMessageBuilder& appendInt16(int16_t num) {
        DataView(_bb.skip(2)).write<BigEndian<int16_t>>(num);
        return *this;
    }

    SqlMessageBuilder& appendInt32(int32_t num) {
        DataView(_bb.skip(4)).write<BigEndian<int32_t>>(num);
        return *this;
    }

    SqlMessageBuilder& appendStr(StringData str) {
        _bb.appendStr(str, true);
        return *this;
    }

    SqlMessageBuilder& appendText(StringData str) {
        _bb.appendStr(str, false);
        return *this;
    }

    SqlMessage done() {
        DataView(_bb.buf() + 1).write<BigEndian<int32_t>>(_bb.len() - 1);
        return {_bb.release()};
    }

    class ScopedSizeBlock {
    public:
        ScopedSizeBlock(BufBuilder& bb) : _bb(bb), _offset(bb.len()) {
            _bb.skip(4);
        }
        ScopedSizeBlock(ScopedSizeBlock&& other)
            : _bb(other._bb), _offset(other._offset), _done(other._done) {
            other._done = true;
        }

        ~ScopedSizeBlock() {
            close();
        }

        void close() {
            if (std::exchange(_done, true))
                return;
            DataView(_bb.buf() + _offset).write<BigEndian<int32_t>>(_bb.len() - _offset - 4);
        }

    private:
        BufBuilder& _bb;
        int _offset;
        bool _done = false;
    };

    ScopedSizeBlock startScopedExclusiveSizeBlock() {
        return ScopedSizeBlock(_bb);
    }

private:
    BufBuilder _bb;
};

class SqlReplySenderImpl final : public SqlReplySender {
public:
    SqlReplySenderImpl(asio::ip::tcp::socket& socket) : _socket(socket) {}

    void sendRowDesc(const std::vector<SqlColumnDesc>& colls) override {
        invariant(_state == kInit);
        _state = kSendingRows;

        auto msg = SqlMessageBuilder(SqlMessage::Kind::kRowDescription);
        msg.appendInt16(colls.size());
        for (auto&& coll : colls) {
            msg.appendStr(coll.name);
            msg.appendInt32(coll.sourceTable);
            msg.appendInt16(coll.sourceColumn);
            msg.appendInt32(PgOid(coll.type));
            msg.appendInt16(coll.typeSize);
            msg.appendInt32(coll.typeMod);
            msg.appendInt16(coll.textOrBinaryFormat);
        }

        asio::write(_socket, msg.done().buffer());
    }

    void sendDataRow(const std::vector<boost::optional<std::string>>& colls) override {
        invariant(_state == kSendingRows);

        auto msg = SqlMessageBuilder(SqlMessage::Kind::kDataRow);
        msg.appendInt16(colls.size());
        for (auto&& coll : colls) {
            if (!coll) {
                msg.appendInt32(-1);
                continue;
            }

            auto sizeBlock = msg.startScopedExclusiveSizeBlock();
            msg.appendText(*coll);
        }

        // TODO buffer rows in memory before sending to the OS.
        asio::write(_socket, msg.done().buffer());
        _nRowsSent++;
    }

    void sendCommandComplete(StringData msg) override {
        invariant(_state == kSendingRows);
        _state = kInit;

        asio::write(_socket,
                    SqlMessageBuilder(SqlMessage::Kind::kCommandComplete)  //
                        .appendStr(msg)
                        .done()
                        .buffer());
    }

    void sendEmptyQueryResponse() override {
        invariant(_state == kInit);

        asio::write(_socket,
                    SqlMessageBuilder(SqlMessage::Kind::kEmptyQuery)  //
                        .done()
                        .buffer());
    }

    int64_t nRowsSent() const override {
        return _nRowsSent;
    }

    bool hasIncompleteCommand() const {
        return _state == kSendingRows;
    }

private:
    enum State {
        kInit,
        kSendingRows,
    };

    int64_t _nRowsSent = 0;
    State _state = kInit;
    asio::ip::tcp::socket& _socket;
};


class SqlNetworkLayerImpl : public SqlNetworkLayer {
public:
    void start(int port = 15432) override {
        _acceptor.open(asio::ip::tcp::v4());
        _acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true));
        _acceptor.bind(asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port));
        _acceptor.listen();
        stdx::thread(&SqlNetworkLayerImpl::acceptThread, this).detach();
    }

    void shutdown() override {
        // TODO
    }

    void acceptThread() {
        while (true) {
            auto sock = _acceptor.accept();
            stdx::thread(&SqlNetworkLayerImpl::clientThread, this, std::move(sock)).detach();
        }
    }

    void clientThread(asio::ip::tcp::socket socket) try {
        ON_BLOCK_EXIT([&] {
            log() << "closing connection to "
                  << transport::endpointToHostAndPort(socket.remote_endpoint());
            socket.close();
        });

        // TODO make this a Decoration on Client?
        StringMap<std::string> clientParams;

        Client::initThread(str::stream() << "sqlconn" << _clientCount.addAndFetch(1));

        log() << "new connection from "
              << transport::endpointToHostAndPort(socket.remote_endpoint());

        // Startup messages
        while (true) {
            const auto msg = readStartupMessage(socket);
            uassert(70006, "StartupMessage is too small", msg.len() >= 8);
            auto cursor = ConstDataRangeCursor(msg.data(), msg.data() + msg.len() - 4);

            const auto protocolVersion =
                uassertStatusOK(cursor.readAndAdvance<BigEndian<int32_t>>());

            if (msg.len() == 8 && protocolVersion == 80877103) {
                // This is the special SSLRequest. For now just report that SSL isn't supported.
                // TODO support SSL.
                const char byte[1] = {'N'};
                asio::write(socket, asio::buffer(byte));
                continue;
            }

            uassert(70007,
                    str::stream() << "bad protocol version: "
                                  << unsignedIntToFixedLengthHex(protocolVersion),
                    (protocolVersion >> 16) == 3);  // Only look at the major version.

            while (true) {
                const StringData key =
                    uassertStatusOK(cursor.readAndAdvance<Terminated<'\0', StringData>>());
                if (key.empty())
                    break;

                uassert(70008,
                        str::stream() << "duplicate client param: " << key,
                        !clientParams.count(key));

                clientParams[key] =
                    StringData(
                        uassertStatusOK(cursor.readAndAdvance<Terminated<'\0', StringData>>()))
                        .toString();
            }

            for (auto&& kv : clientParams) {
                log() << "client param '" << kv.first << "' = '" << kv.second << "'";
            }

            uassert(
                70009, "Missing required 'database' client param", clientParams.count("database"));

            // TODO real authN.
            asio::write(socket,
                        SqlMessageBuilder(SqlMessage::Kind::kAuth)
                            .appendInt32(0)  // AuthOK
                            .done()
                            .buffer());

            asio::write(socket,
                        SqlMessageBuilder(SqlMessage::Kind::kReadyForQuery)
                            .appendByte('I')  // (I)dle
                            .done()
                            .buffer());

            break;
        }

        // Main client loop
        while (true) {
            auto msg = readNormalMessage(socket);
            if (msg.kind() == SqlMessage::Kind::kTerminate) {
                invariant(msg.len() == 4);
                // TODO actively rollback open transactions.
                return;
            }
            if (msg.kind() != SqlMessage::Kind::kSimpleQuery) {
                log() << "unsupported message kind " << char(msg.kind());
                return;
            }

            BufReader reader(msg.data(), msg.len() - 4);
            const auto query = reader.readCStr().toString();
            invariant(!reader.remaining());
            log() << "query: " << query;

            SqlReplySenderImpl replySender(socket);


            {
                auto opCtx = cc().makeOperationContext();
                runSQL2(opCtx.get(), clientParams.find("database")->second, query, &replySender);
            }

            invariant(!replySender.hasIncompleteCommand());

            asio::write(socket,
                        SqlMessageBuilder(SqlMessage::Kind::kReadyForQuery)
                            .appendByte('I')  // (I)dle
                            .done()
                            .buffer());
        }
    } catch (const DBException& ex) {
        log() << "DBException thrown: " << ex.toString();
    } catch (const std::exception& ex) {
        log() << "std::exception (" << demangleName(typeid(ex)) << ") thrown: " << ex.what();
    }

    static SqlMessage readNormalMessage(asio::ip::tcp::socket& socket) {
        char sizeBuf[5];
        invariant(asio::read(socket, asio::buffer(sizeBuf)) == 5);
        const auto size = ConstDataView(sizeBuf + 1).read<BigEndian<int32_t>>();
        uassert(70005,
                str::stream() << "invalid message size: " << size,
                size >= 4 && size < 64 * 1024 * 1024);

        auto buf = SharedBuffer::allocate(size + 1);
        memcpy(buf.get(), sizeBuf, 5);
        asio::read(socket, asio::buffer(buf.get() + 5, size - 4));
        return {std::move(buf)};
    }

    static SqlMessage readStartupMessage(asio::ip::tcp::socket& socket) {
        char sizeBuf[4];
        asio::read(socket, asio::buffer(sizeBuf));
        const auto size = ConstDataView(sizeBuf).read<BigEndian<int32_t>>();
        uassert(70004,
                str::stream() << "invalid message size: " << size,
                size >= 4 && size < 64 * 1024 * 1024);

        auto buf = SharedBuffer::allocate(size + 1);
        *buf.get() = char(SqlMessage::Kind::kStartup);
        memcpy(buf.get() + 1, sizeBuf, 4);
        asio::read(socket, asio::buffer(buf.get() + 5, size - 4));
        return {std::move(buf)};
    }

    AtomicUInt64 _clientCount;

    asio::io_context _ioCtx;
    asio::ip::tcp::acceptor _acceptor{_ioCtx};
};
}

const ServiceContext::Decoration<std::unique_ptr<SqlNetworkLayer>> SqlNetworkLayer::get =
    ServiceContext::declareDecoration<std::unique_ptr<SqlNetworkLayer>>();

std::unique_ptr<SqlNetworkLayer> makeSqlNetworkLayer() {
    return std::make_unique<SqlNetworkLayerImpl>();
}
}
