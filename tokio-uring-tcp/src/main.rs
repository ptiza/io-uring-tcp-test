use clap::Parser;
use std::net::{Ipv4Addr, SocketAddr};
use apache_avro::{Codec, Schema, Writer, Reader};
use apache_avro::types::Record;
use tokio_uring::net::{TcpStream, TcpListener};


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


    tokio_uring::start(async {

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
        let stream = TcpStream::connect(std_addr).await.unwrap();

        // write data
        let (result, _) = stream.write(encoded).await;
        result?;
        Ok(())
    })
}

fn run_client(args: Args) -> std::io::Result<()> {
    tokio_uring::start(async {
        let socket_addr: SocketAddr = format!("{}:50001", args.bind_addr).parse().unwrap();
        let listener = TcpListener::bind(socket_addr).unwrap();

        println!("Listening on {}", listener.local_addr().unwrap());

        loop {
            let (stream, socket_addr) = listener.accept().await.unwrap();
            tokio_uring::spawn(async move {

                println!("{} connected", socket_addr);
                let mut n = 0;

                let mut buf = vec![0u8; 4096];
                loop {
                    let (result, nbuf) = stream.read(buf).await;
                    buf = nbuf;
                    let read = result.unwrap();
                    if read == 0 {
                        buf.truncate(n);
                        Codec::Zstandard.decompress(&mut buf).unwrap();
                        let reader = Reader::new(&buf[..]).unwrap();
                        // value is a Result  of an Avro Value in case the read operation fails
                        for value in reader {
                            match value {
                                Ok(v) => println!("{:?}", v),
                                Err(_) => break,
                            };
                        }
                        println!("{} close, {} bytes total received", socket_addr, n);
                        break;
                    }
                    n += read;
                }
            });
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
