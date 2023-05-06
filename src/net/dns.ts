import dns from "dns";

export function dnsLoopUp(host: string): Promise<string> {
  return new Promise((resolve, reject) => {
    dns.lookup(host, (err, addr) => {
      err ? reject(err) : resolve(addr);
    });
  });
}