# consul-proxy
A TCP proxy which uses Consul for discovery of upstream servers.

## Usage
`consul-proxy --listen 0.0.0.0:8000 --service backend`

## TODO
- Better error handling
- Plugable logging
- Support for different load balancing algorithms
- Catch signals for graceful shutdown
