import net from "net";

export class ConnCreateError extends Error {
  constructor(msg: any) { super(msg) }
}

export var createConnection = async (
  port: number,
  host?: string
): Promise<net.Socket> => {
  var conn = net.createConnection(port, host);
  return new Promise((resolve, reject) => {
    conn.once("connect", () => {
      resolve(conn);
      conn.removeAllListeners("error");
    });
    conn.once("error", (err) => {
      reject(new ConnCreateError(err));
      conn.removeAllListeners("connect");
    });
  });
};

export var createServer = async (
  port: number,
  host: string,
  options?: net.ServerOpts
): Promise<net.Server> => {
  var srv = net.createServer(options);
  srv.listen(port, host);
  return new Promise((resolve, reject) => {
    srv.once("listening", () => {
      resolve(srv);
      srv.removeAllListeners("error");
    });
    srv.once("error", (err) => {
      reject(new ConnCreateError(err));
      srv.removeAllListeners("connect");
    });
  });
};
