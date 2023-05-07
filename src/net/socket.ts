import EventEmitter from "events";
import { Socket } from "net";

const socketReadEvent = "socket_read";
const socketErrorEvent = "socket_error";

const SOCKET_ERRORS = {
  ERROR_SEND_ON_CLOSED_SOCKET: new Error("send on closed socket"),
  ERROR_WRITE_ON_CLOSED_SOCKET: new Error("write on closed socket"),
};

interface SocketEvent {
  n: number;
  emitter: EventEmitter;
}

export class SocketPromise {
  _sock: Socket;
  private waitingReadEvents: SocketEvent[] = [];
  private readBuffer: Buffer | undefined;

  private _hasError: boolean = false;
  private _hasClosed: boolean = false;
  private _hasEnd: boolean = false;

  constructor(socket: Socket) {
    this._sock = socket
      .on("data", this.onData)
      // Emitted when an error occurs. The 'close' event will be called directly following this event.
      .on("error", this.onError)
      // Emitted when the other end of the socket signals the end of transmission, thus ending the readable side of the socket.
      .on("end", this.onEnd);
  }

  stopWatchEvents = () => {
    this._sock
      .removeListener("data", this.onData)
      .removeListener("error", this.onError)
      .removeListener("end", this.onEnd);
  };

   read(n?: number): Promise<Buffer> {
    if (this._hasError || this._hasEnd) {
      return Promise.reject(SOCKET_ERRORS.ERROR_SEND_ON_CLOSED_SOCKET);
    }

    if (this.readBuffer && this.waitingReadEvents.length === 0) {
      if (n && this.readBuffer.length >= n) {
        return Promise.resolve(this.getN(n));
      }
      if (!n) {
        return Promise.resolve(this.getN(this.readBuffer.length));
      }
    }

    var emitter = new EventEmitter();
    this.waitingReadEvents.push({ n: n ? n : -1, emitter: emitter });

    return new Promise((resolve, reject) => {
      emitter.on(socketReadEvent, (response) => resolve(response));
      emitter.on(socketErrorEvent, (err) => reject(err));
    });
  };

  write(buffer: Uint8Array | string): Promise<void>  {
    if (this._hasClosed || this._hasError) {
      return Promise.reject(SOCKET_ERRORS.ERROR_WRITE_ON_CLOSED_SOCKET);
    }

    var emitter = new EventEmitter();
    this._sock.write(buffer, (err) => emitter.emit(socketErrorEvent, err));

    return new Promise((resolve, reject) => {
      emitter.on(socketErrorEvent, (err) => {
        err ? reject(err) : resolve();
      });
    });
  };

  close = () => {
    this._sock.writable ? this._sock.end() : null;
    this._hasClosed = true;
  };

  private onData = (buffer: Buffer) => {
    this.readBuffer = this.readBuffer
      ? Buffer.concat([this.readBuffer, buffer])
      : buffer;

    while (
      this.waitingReadEvents.length > 0 &&
      this.waitingReadEvents[0].n <= this.readBuffer.length
    ) {
      var ev = this.waitingReadEvents[0];
      this.waitingReadEvents = this.waitingReadEvents.slice(1);

      var response =
        ev.n === -1
          ? this.getN(this.readBuffer.length)
          : this.getN(ev.n);
      ev.emitter.emit(socketReadEvent, response);
    }
  };

  private onError = (err: Error) => {
    this.waitingReadEvents.map((__waitingReadEvent) => {
      __waitingReadEvent.emitter?.emit(socketErrorEvent, err);
    });
    this.waitingReadEvents = [];
    this._hasError = true;
  };

  private onEnd = () => (this._hasEnd = true);

  private getN = (n: number): Buffer => {
    if (!this.readBuffer) {
      throw new Error("readBuffer should not be undefined");
    }

    var response: Buffer;

    if (this.readBuffer.length === n) {
      response = this.readBuffer;
      this.readBuffer = undefined;
    } else {
      response = this.readBuffer.subarray(0, n);
      this.readBuffer = this.readBuffer.subarray(n);
    }

    return response;
  };
}
