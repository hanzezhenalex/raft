import { Client } from "./proxy/client";
import { program } from "commander";

var clientCommand = program.command("client");

clientCommand
  .requiredOption("--server-ip <value>", "socks5 server ip")
  .option("--server-port <value>", "socks5 server port", "1080")
  .option("--client-ip <value>", "client ip", "localhost")
  .option("--client-port <value>", "client port", "1080")
  .action(async (options, command) => {
    var clientPort = Number(options.clientPort);
    var serverPort = Number(options.serverPort);
    var cfg = {
      clientIP: options.clientIp,
      clientPort: clientPort,
      serverIP: options.serverIp,
      serverPort: serverPort,
    };
    console.log(`client started. cfg=${JSON.stringify(cfg)}`);
    var client = new Client(cfg);
    await client.Start()
  });

program.parse(process.argv);
