use robocodec::FormatReader;
use robocodec::io::formats::bag::BagFormat;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = "/Users/zhexuany/Downloads/leju_bag/Rubbish_sorting_P4-278_20250830101814.bag";
    let reader = BagFormat::open(path)?;

    println!("Opened bag file");
    println!("Channels: {}", reader.channels().len());
    println!("Total messages: {}", reader.message_count());

    // Try to decode messages
    let decoded_iter = reader.decode_messages()?;
    let mut stream = decoded_iter.stream()?;

    let mut count = 0;
    let mut errors = 0;
    let mut metadata_count = 0;

    for result in &mut stream {
        match result {
            Ok((msg, channel)) => {
                count += 1;
                if channel.topic.contains("metadata") {
                    metadata_count += 1;
                    if metadata_count <= 3 {
                        println!(
                            "Metadata message {}: topic={}, fields={}",
                            metadata_count,
                            channel.topic,
                            msg.len()
                        );
                    }
                }
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
                errors += 1;
                if errors <= 5 {
                    eprintln!("Error {}: {}", errors, e);
                }
            }
        }
        if count >= 100 || errors >= 100 {
            break;
        }
    }

    println!("\nSuccessfully decoded {} messages", count);
    println!("Metadata messages decoded: {}", metadata_count);
    println!("Total errors: {}", errors);

    Ok(())
}
