# io-uring-tcp-test
io_uring proof of concept for TCP with glommio & tokio-uring, using zstd compressed Avro records
---

Features a client (receiver) and a server (sender).

Transmits a number of Avro records which contain some data. Transmission is zstd compressed.

```sh
# build
cargo build --release
# binaries in:
cd target/release
```


### Example:
- In one terminal window, to start client, execute:
    - `./tokio-uring-tcp`
- In another terminal window, to start server, execute:
    - `.tokio-uring-tcp -s`


### CLI arguments:
```
Options:
  -s, --server
  -b, --bind-addr <BIND_ADDR>  [default: 0.0.0.0]
  -c, --count <COUNT>          [default: 1]
  -h, --help                   Print help
  -V, --version                Print version
```
