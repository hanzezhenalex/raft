import { Client } from "./client/client";

var client = new Client({
    clientIp: "localhost",
    clientPort: 8010,
    serverIp: "localhost",
    serverPort: 8011,
})

client.Start()