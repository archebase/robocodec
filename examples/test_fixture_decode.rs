use robocodec::io::formats::bag::BagFormat;
use std::path::Path;

fn main() {
    let bag_path = "tests/fixtures/robocodec_test_15.bag";

    if !Path::new(bag_path).exists() {
        println!("Fixture file not found");
        return;
    }

    let reader = BagFormat::open(bag_path).expect("Failed to open BAG file");

    // Get raw messages using iter_raw
    let raw_iter = reader.iter_raw().expect("Failed to get raw iterator");

    // Print first few messages with raw data
    for (idx, result) in raw_iter.enumerate() {
        if idx >= 5 {
            break;
        }
        match result {
            Ok((msg, channel)) => {
                println!("\n=== Message {} ===", idx + 1);
                println!("Topic: {}", channel.topic);
                println!("Type: {}", channel.message_type);
                println!("Data length: {} bytes", msg.data.len());

                // Print first 64 bytes as hex
                let hex: Vec<String> = msg
                    .data
                    .iter()
                    .take(64)
                    .map(|b| format!("{:02x}", b))
                    .collect();
                println!("First 64 bytes: {}", hex.join(" "));

                // Try to interpret as ROS1 message with header
                if msg.data.len() >= 16 {
                    let seq =
                        u32::from_le_bytes([msg.data[0], msg.data[1], msg.data[2], msg.data[3]]);
                    let sec =
                        u32::from_le_bytes([msg.data[4], msg.data[5], msg.data[6], msg.data[7]]);
                    let nsec =
                        u32::from_le_bytes([msg.data[8], msg.data[9], msg.data[10], msg.data[11]]);
                    let str_len = u32::from_le_bytes([
                        msg.data[12],
                        msg.data[13],
                        msg.data[14],
                        msg.data[15],
                    ]);
                    println!("Interpreted as ROS1 header:");
                    println!("  seq: {}", seq);
                    println!("  stamp.sec: {}", sec);
                    println!("  stamp.nsec: {}", nsec);
                    println!("  frame_id length: {}", str_len);

                    if str_len < 1000 && (16 + str_len as usize) <= msg.data.len() {
                        let frame_id =
                            String::from_utf8_lossy(&msg.data[16..16 + str_len as usize]);
                        println!("  frame_id: \"{}\"", frame_id);
                    }
                }
            }
            Err(e) => {
                println!("\n=== Message {} (ERROR) ===", idx + 1);
                println!("Error: {:?}", e);
            }
        }
    }
}
