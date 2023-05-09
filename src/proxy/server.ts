import net, { AddressInfo } from "net"
import { ConnCreateError, createConnection, createServer } from "../net/create"
import { dnsLoopUp } from "../net/dns"
import { pipe } from "../net/pipe"
import { Addressing, Authentication, Errors, MethodSelection, SOCKS5_VERSION, STAGE } from "../socks/handshake"
import { ClientConn, ServerConn } from "./common"

interface Config {
    ip: string,
    port: number,
}

export class Server {
    private _srv: net.Server | undefined
    private _cfg: Config

    constructor(cfg: Config) {
        this._cfg = cfg
    }

    start = async () => {
        this._srv = await createServer(this._cfg.port, this._cfg.ip)
        this._srv.on('connection', this.onConnection)
    }

    onConnection = async (socket: net.Socket) => {
        var from = new ClientConn(socket)
        var to: ServerConn | undefined;
        var stage: STAGE = STAGE.PREPARING;

        try {
            // version identify and method selection
            stage = STAGE.ADDRESSING;
            var methodRequest = await MethodSelection.readReq(from);

            var handler = Authentication.selectAuthMethod(methodRequest.getMethod())
            if (!handler) {
                await from.write(Authentication.NO_ACCEPTABLE_METHODS);
                throw new Error(
                    `Auth method not found, supportted methods=${methodRequest.getMethod()}`
                );
            }
            await from.write(new Uint8Array([SOCKS5_VERSION, handler.method]))

            stage = STAGE.AUTHENTICATION;
            handler.serverHandler?.apply(this, from)

            stage = STAGE.ADDRESSING;
            var addrRequest = await Addressing.readMessage(from);
            
            if (addrRequest.getCmdOrRep() != Addressing.CONNECT) {
                throw new Errors.UnsupporttedMethod(addrRequest.getCmdOrRep())
            }

            // try to create connection to target server
            to = new ServerConn(await createConnection(
                addrRequest.getTargetPort(),
                addrRequest.needDnsLookUp()
                    ? await dnsLoopUp(addrRequest.getTargetAddr())
                    : addrRequest.getTargetAddr()
            ))
            
            var bindAddrInfo: AddressInfo | {} = to._sock.address();
            var addrRep = new Addressing.Message(Addressing.SUCCEED,
                (bindAddrInfo as AddressInfo).family === "IPv4" ? 0x00 : 0x04,
                (bindAddrInfo as AddressInfo).family === "IPv4" ? 4 : 16,
                stringToUint8Array((bindAddrInfo as AddressInfo).address),
                (bindAddrInfo as AddressInfo).port)

            await from.write(addrRep.toBuffer());

            stage = STAGE.PIPING;
            from.stopWatchEvents();
            to.stopWatchEvents();
            pipe(from._sock, to._sock);
        } catch (e) {
            console.error(e);

            if (stage === STAGE.ADDRESSING && (e as ConnCreateError)) {
                if (from._sock.writable) {
                    await from.write(Addressing.SERVER_NOT_AVAIABLE)
                }
            }

            if (stage === STAGE.ADDRESSING && (e as Errors.UnsupporttedMethod)) {
                if (from._sock.writable) {
                    await from.write(Addressing.METHOD_NOT_SUPPORT)
                }
            }

            from.close();
            to?.close();
            return;
        }

    }

}


function stringToUint8Array(str) {
    var arr = [];
    for (var i = 0, j = str.length; i < j; ++i) {
        arr.push(str.charCodeAt(i));
    }

    var tmpUint8Array = new Uint8Array(arr);
    return tmpUint8Array
}