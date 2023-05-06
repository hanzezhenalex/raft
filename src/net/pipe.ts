import { Socket } from "net";

export var Pipe = (sock1: Socket, sock2: Socket) => {
    sock1.pipe(sock2).pipe(sock1)
    var __close_socks = () => {
        sock1.writable ? sock1.end(): null;
        sock2.writable ? sock2.end(): null;
    }
    sock1.on('error', (err: Error) => {
        __close_socks()
    })
    sock2.on('error', (err: Error) => {
        __close_socks()
    })
}