#!/bin/bash

set -x 

# In curl >= 7.18.0, use
# curl http://www.baidu.com --socks5-hostname localhost:8010

# In curl >= 7.21.7, use
curl http://www.baidu.com -x socks5h://localhost:8010
