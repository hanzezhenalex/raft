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
  __sock: Socket;
  private __waiting_read_events: SocketEvent[] = [];
  private __read_buffer: Buffer | undefined;

  private __has_error: boolean = false;
  private __has_closed: boolean = false;
  private __has_end: boolean = false;

  constructor(socket: Socket) {
    this.__sock = socket
      .on("data", this.__onData)
      // Emitted when an error occurs. The 'close' event will be called directly following this event.
      .on("error", this.__onError)
      // Emitted when the other end of the socket signals the end of transmission, thus ending the readable side of the socket.
      .on("end", this.__onEnd);
  }

  stopWatchEvents = () => {
    this.__sock
      .removeListener("data", this.__onData)
      .removeListener("error", this.__onError)
      .removeListener("end", this.__onEnd);
  };

  read = (n?: number): Promise<Buffer> => {
    if (this.__has_error || this.__has_end) {
      return Promise.reject(SOCKET_ERRORS.ERROR_SEND_ON_CLOSED_SOCKET);
    }

    if (this.__read_buffer && this.__waiting_read_events.length === 0) {
      if (n && this.__read_buffer.length >= n) {
        return Promise.resolve(this.__getN(n));
      }
      if (!n) {
        return Promise.resolve(this.__getN(this.__read_buffer.length));
      }
    }

    var emitter = new EventEmitter();
    this.__waiting_read_events.push({ n: n ? n : -1, emitter: emitter });

    return new Promise((resolve, reject) => {
      emitter.on(socketReadEvent, (response) => resolve(response));
      emitter.on(socketErrorEvent, (err) => reject(err));
    });
  };

  write = (buffer: Uint8Array | string): Promise<void> => {
    if (this.__has_closed || this.__has_error) {
      return Promise.reject(SOCKET_ERRORS.ERROR_WRITE_ON_CLOSED_SOCKET);
    }

    var emitter = new EventEmitter();
    this.__sock.write(buffer, (err) => emitter.emit(socketErrorEvent, err));

    return new Promise((resolve, reject) => {
      emitter.on(socketErrorEvent, (err) => {
        err ? reject(err) : resolve();
      });
    });
  };

  close = () => {
    this.__sock.writable ? this.__sock.end() : null;
    this.__has_closed = true;
  };

  private __onData = (buffer: Buffer) => {
    this.__read_buffer = this.__read_buffer
      ? Buffer.concat([this.__read_buffer, buffer])
      : buffer;

    while (
      this.__waiting_read_events.length > 0 &&
      this.__waiting_read_events[0].n <= this.__read_buffer.length
    ) {
      var ev = this.__waiting_read_events[0];
      this.__waiting_read_events = this.__waiting_read_events.slice(1);

      var response =
        ev.n === -1
          ? this.__getN(this.__read_buffer.length)
          : this.__getN(ev.n);
      ev.emitter.emit(socketReadEvent, response);
    }
  };

  private __onError = (err: Error) => {
    this.__waiting_read_events.map((__waitingReadEvent) => {
      __waitingReadEvent.emitter?.emit(socketErrorEvent, err);
    });
    this.__waiting_read_events = [];
    this.__has_error = true;
  };

  private __onEnd = () => (this.__has_end = true);

  private __getN = (n: number): Buffer => {
    if (!this.__read_buffer) {
      throw new Error("__readBuffer should not be undefined");
    }

    var response: Buffer;

    if (this.__read_buffer.length === n) {
      response = this.__read_buffer;
      this.__read_buffer = undefined;
    } else {
      response = this.__read_buffer.subarray(0, n);
      this.__read_buffer = this.__read_buffer.subarray(n);
    }

    return response;
  };
}
