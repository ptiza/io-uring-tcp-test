use apache_avro::types::Record;
use apache_avro::{Codec, Reader, Schema, Writer};
use clap::Parser;
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::net::{TcpListener, TcpStream};
use glommio::LocalExecutor;
use std::net::{Ipv4Addr, SocketAddr};

pub const MAX_DATAGRAM_SIZE: usize = 65507;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, disable_colored_help = true)]
struct Args {
    #[arg(short, long, default_value_t = false)]
    server: bool,

    #[arg(short, long, default_value_t = Ipv4Addr::UNSPECIFIED)]
    bind_addr: Ipv4Addr,

    #[arg(short, long, default_value_t = 1)]
    count: usize,
}

fn run_server(args: Args) -> std::io::Result<()> {
    let raw_schema = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "a", "type": "long", "default": 42},
                {"name": "b", "type": "string"}
            ]
        }
    "#;

    let schema = Schema::parse_str(raw_schema).unwrap();

    let ex = LocalExecutor::default();
    ex.run(async move {
        // a writer needs a schema and something to write to
        let mut writer = Writer::new(&schema, Vec::new());

        // the Record type models our Record schema
        for _ in 1..=args.count {
            let mut record = Record::new(writer.schema()).unwrap();
            record.put("a", 27i64);
            record.put("b", "foo");

            // schema validation happens here
            writer.append(record).unwrap();
        }
        // this is how to get back the resulting avro bytecode
        let mut encoded = writer.into_inner().unwrap();
        Codec::Zstandard.compress(&mut encoded).unwrap();
        let std_addr: SocketAddr = format!("{}:50001", args.bind_addr).parse().unwrap();
        let mut stream = TcpStream::connect(std_addr).await.unwrap();

        // write data
        stream.write(&encoded).await?;
        Ok(())
    })
}

fn run_client(args: Args) -> std::io::Result<()> {
    let ex = LocalExecutor::default();
    ex.run(async move {
        let socket_addr: SocketAddr = format!("{}:50001", args.bind_addr).parse().unwrap();
        let listener = TcpListener::bind(socket_addr).unwrap();

        println!("Listening on {}", listener.local_addr().unwrap());

        loop {
            let mut stream = listener.accept().await.unwrap();
            glommio::spawn_local(async move {
                println!("{} connected", stream.peer_addr().unwrap());
                let mut received_bytes: Vec<u8> = vec![];

                let mut buf = [0u8; 4096];
                loop {
                    let result = stream.read(&mut buf).await.unwrap();
                    received_bytes.extend_from_slice(&buf[..result]);
                    if result == 0 {
                        let n_total = received_bytes.len();
                        Codec::Zstandard.decompress(&mut received_bytes).unwrap();
                        let reader = Reader::new(&received_bytes[..]).unwrap();
                        // value is a Result  of an Avro Value in case the read operation fails
                        for value in reader {
                            match value {
                                Ok(v) => println!("{:?}", v),
                                Err(_) => break,
                            };
                        }
                        println!(
                            "{} close, {} bytes total received",
                            stream.peer_addr().unwrap(),
                            n_total
                        );
                        break;
                    }
                }
            })
            .detach();
        }
    });
    Ok(())
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();

    match args.server {
        true => run_server(args),
        false => run_client(args),
    }
}
