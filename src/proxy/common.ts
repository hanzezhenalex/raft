import { SocketPromise } from "../net/socket";
import net from "net";

export class ClientConnError extends Error {
  constructor(msg: any) {
    super(msg);
  }
}

export class ClientConn extends SocketPromise {
  constructor(socket: net.Socket) {
    super(socket);
  }

   async read(n?: number): Promise<Buffer> {
    try {
      return await super.read(n);
    } catch (e) {
      throw new ClientConnError(e);
    }
  };

  async  write(buffer: Uint8Array | string): Promise<void> {
    try {
      return await super.write(buffer);
    } catch (e) {
      throw new ClientConnError(e);
    }
  };
}

export class ServerConnError extends Error {
  constructor(msg: any) {
    super(msg);
  }
}

export class ServerConn extends SocketPromise {
  constructor(socket: net.Socket) {
    super(socket);
  }

  async read(n?: number): Promise<Buffer> {
    try {
      return await super.read(n);
    } catch (e) {
      throw new ServerConnError(e);
    }
  };

   async write(buffer: Uint8Array | string): Promise<void> {
    try {
      return await super.write(buffer);
    } catch (e) {
      throw new ServerConnError(e);
    }
  };
}
