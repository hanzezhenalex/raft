import net from "net";
import {
  Addressing,
  MethodSelection,
  STAGE,
  Authentication,
} from "../socks/handshake";
import { pipe } from "../net/pipe";
import { createConnection, createServer } from "../net/create";
import { ClientConn, ServerConn, ServerConnError } from "./common";

interface Config {
  clientIP: string;
  clientPort: number;

  serverIP: string;
  serverPort: number;
}

export class Client {
  private _cfg: Config;
  private _srv: net.Server | undefined;

  constructor(cfg: Config) {
    this._cfg = cfg;
  }

  start = async () => {
    this._srv = await createServer(this._cfg.clientPort, this._cfg.clientIP);
    this._srv.on("connection", this.onConnection);
  };

  private newConnToSocksServer = async (): Promise<ServerConn> => {
    var conn = await createConnection(this._cfg.serverPort, this._cfg.serverIP);
    return new ServerConn(conn);
  };

  private onConnection = async (socket: net.Socket) => {
    var from = new ClientConn(socket);
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

      to = await this.newConnToSocksServer();

      await to.write(methodRequest.toBuffer());
      var methodRep = await MethodSelection.readReply(to);

      var handler = Authentication.getAuthHandler(methodRep.getMethod());
      if (!handler) {
        await from.write(Authentication.NO_ACCEPTABLE_METHODS);
        throw new Error(
          `Auth method not found, method=${methodRep.getMethod()}`
        );
      }
      await from.write(methodRep.toBuffer());

      // auth check
      stage = STAGE.AUTHENTICATION;
      await handler.clientHandler?.call(from, to);
      

      stage = STAGE.ADDRESSING;
      var addrRequest = await Addressing.readMessage(from);
      await to.write(addrRequest.toBuffer());

      var addrRep = await Addressing.readMessage(to);
      await from.write(addrRep.toBuffer());

      if (addrRep.getCmdOrRep() !== Addressing.SUCCEED) {
        throw new Error(`Address fails, reason=${addrRep.getCmdOrRep()}`);
      }
    } catch (e) {
      console.log(e);

      if (stage === STAGE.ADDRESSING && (e as ServerConnError)) {
        if (from._sock.writable) {
          await from.write(Addressing.SERVER_INTERNAL_ERROR);
        }
      }

      from.close();
      to?.close();
      return;
    }

    stage = STAGE.PIPING;
    from.stopWatchEvents();
    to.stopWatchEvents();
    pipe(from._sock, to._sock);
  };
}
