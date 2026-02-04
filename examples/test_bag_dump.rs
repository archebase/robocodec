use robocodec::io::formats::bag::BagFormat;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = "/Users/zhexuany/Downloads/leju_bag/Rubbish_sorting_P4-278_20250830101814.bag";
    let reader = BagFormat::open(path)?;

    let mut iter = reader.iter_raw()?;

    // Look at first few messages
    for i in 0..5 {
        if let Some(Ok((msg, channel))) = iter.next() {
            println!(
                "Message {}: topic={}, data_len={}",
                i,
                channel.topic,
                msg.data.len()
            );
            println!(
                "  First 32 bytes: {:02x?}",
                &msg.data[..msg.data.len().min(32)]
            );
        }
    }

    Ok(())
}
