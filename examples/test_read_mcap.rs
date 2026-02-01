use robocodec::io::formats::mcap::McapFormat;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mcap_path = "/tmp/leju_bag.mcap";

    let reader = McapFormat::open(mcap_path)?;
    println!("Opened MCAP file");
    println!("Channels: {}", reader.channels().len());

    // Try to decode messages
    let decoded_iter = reader.decode_messages()?;
    let mut stream = decoded_iter.stream()?;

    let mut count = 0;
    for result in &mut stream {
        match result {
            Ok((msg, channel)) => {
                count += 1;
                if count <= 5 {
                    println!(
                        "Message {}: topic={}, fields={}",
                        count,
                        channel.topic,
                        msg.len()
                    );
                }
            }
            Err(e) => {
                if count < 5 {
                    eprintln!("Error {}: {}", count + 1, e);
                }
            }
        }
        if count >= 100 {
            break;
        }
    }

    println!("\nSuccessfully decoded {} messages from MCAP", count);
    Ok(())
}
