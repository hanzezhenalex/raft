import net from "net";

export var CreateConnection = async (
  port: number,
  host?: string
): Promise<net.Socket> => {
  var __sock = net.createConnection(port, host);
  return new Promise((resolve, reject) => {
    __sock.once("connect", () => {
      resolve(__sock);
      __sock.removeAllListeners("error");
    });
    __sock.once("error", (err) => {
      reject(err);
      __sock.removeAllListeners("connect");
    });
  });
};

export var CreateServer = async (
  port: number,
  host: string,
  options?: net.ServerOpts
): Promise<net.Server> => {
  var __srv = net.createServer(options);
  __srv.listen(port, host);
  return new Promise((resolve, reject) => {
    __srv.once("listening", () => {
      resolve(__srv);
      __srv.removeAllListeners("error");
    });
    __srv.once("error", (err) => {
      reject(err);
      __srv.removeAllListeners("connect");
    });
  });
};
