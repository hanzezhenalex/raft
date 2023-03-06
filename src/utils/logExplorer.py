import re
from http.server import SimpleHTTPRequestHandler, HTTPServer
import argparse

class LogEntry:
    def __init__(self, time: str, id: str, log: str) -> None:
        self.id = id
        self.log = log
        self.time = time

    def gen(self, max_id: int):
        id = int(self.id)
    
        if id == -1:
            return "<table class='tableStyle'><td>{}</td>".format(self.log)
        
        template = "<table class='tableStyle'><td>{}</td>".format(self.time)
        for i in range(max_id+1):
            if id == i:
                template += "<td class='td-id'>{}</td>".format(self.log)
            else:
                template += "<td class='td-id'></td>"
        template += "</table>"
        return template

# time="2023-03-06T19:47:04+08:00" level=debug msg="raft start with election timeout = 162ms" id=2
pattern = r"time=\"(.*)\" level=debug msg=\"(.*)\"(.*)id=(\d)(.*)"

class Reader:
    def __init__(self, path) -> None:
        self.path = path
        self.entries = []
        self.max_id = "-1"
    
    def parse(self):
        with open(self.path) as f:
            while True:
                line = f.readline()
                if line == "":
                    break
                if not line.startswith("time"):
                    self.entries.append(LogEntry("", "-1", line))
                    continue
                res = re.match(pattern, line, flags=0)
                time = res.group(1)
                msg = res.group(2)
                extra1 = res.group(3)
                id = res.group(4)
                extra2 = res.group(5)
                if id > self.max_id:
                    self.max_id = id
                self.entries.append(LogEntry(time, id, extra1 + msg + extra2))
        return self

    def gen(self):
        body = ""
        for i in self.entries:
            body += i.gen(int(self.max_id))
        body =  "<!DOCTYPE html><body>{}</body>".format(body)
        return body + style()

def style():
    ret = r".tableStyle{width: 100%;word-wrap:bread-word;word-break:break-all;table-layout:fixed;}"
    ret += r" .td-id {font-size:20px;height: auto;border: 1px solid #1f8cfa;}</style>"
    # ret += r" .td-main {font-size:20px;height: auto;border: 1px solid #1f8cfa; background-color: #1f8cfa}</style>"
    return "<style>{}<\style>".format(ret)

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--log", '-l')
    parser.add_argument("--out", '-o')

    args = parser.parse_args()

    ret = Reader(args.log).parse().gen()
    with open(args.out, 'w') as f:
        f.write(ret)

if __name__ == "__main__":  
    main()