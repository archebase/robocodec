use robocodec::encoding::CdrDecoder;
use robocodec::io::formats::bag::BagFormat;
use robocodec::schema::parse_schema;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = "/Users/zhexuany/Downloads/leju_bag/Rubbish_sorting_P4-278_20250830101814.bag";
    let reader = BagFormat::open(path)?;

    let mut iter = reader.iter_raw()?;

    // Find a simple message to debug
    while let Some(Ok((msg, channel))) = iter.next() {
        // Try the metadata message which has a simple structure
        if channel.topic.contains("metadata") {
            println!("Topic: {}", channel.topic);
            println!("Type: {}", channel.message_type);
            println!("Data length: {}", msg.data.len());
            println!(
                "First 64 bytes: {:02x?}",
                &msg.data[..msg.data.len().min(64)]
            );

            // Parse the schema
            if let Some(schema_str) = &channel.schema {
                println!("\nSchema:\n{}", schema_str);

                // Try to parse and decode
                match parse_schema(&channel.message_type, schema_str) {
                    Ok(schema) => {
                        println!("\nParsed schema successfully");
                        println!(
                            "Schema types: {:?}",
                            schema.types.keys().collect::<Vec<_>>()
                        );

                        // Try decoding
                        let decoder = CdrDecoder::new();
                        match decoder.decode_headerless_ros1(
                            &schema,
                            &msg.data,
                            Some(&channel.message_type),
                        ) {
                            Ok(decoded) => {
                                println!("\nDecoded successfully!");
                                for (k, v) in decoded.iter() {
                                    println!("  {}: {:?}", k, v);
                                }
                            }
                            Err(e) => {
                                println!("\nDecode error: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        println!("\nSchema parse error: {}", e);
                    }
                }
            }
            break;
        }
    }

    Ok(())
}
