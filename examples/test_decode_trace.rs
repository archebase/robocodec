use robocodec::encoding::cdr::cursor::CdrCursor;
use robocodec::io::formats::bag::BagFormat;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = "/Users/zhexuany/Downloads/leju_bag/Rubbish_sorting_P4-278_20250830101814.bag";
    let reader = BagFormat::open(path)?;

    let mut iter = reader.iter_raw()?;

    // Find a metadata message
    for result in &mut iter {
        let Ok((msg, channel)) = result else { continue };
        if !channel.topic.contains("metadata") {
            continue;
        }

        println!("Topic: {}", channel.topic);
        println!("Data length: {}", msg.data.len());

        // Create a ROS1 cursor and manually decode
        let mut cursor = CdrCursor::new_headerless_ros1(&msg.data, true);

        println!("\nManual decoding:");
        println!("is_ros1: {}", cursor.is_ros1());

        // Read Header.seq (uint32)
        let seq = cursor.read_u32()?;
        println!("Header.seq = {} (offset now: {})", seq, cursor.position());

        // Read Header.stamp.sec (int32)
        let stamp_sec = cursor.read_i32()?;
        println!(
            "Header.stamp.sec = {} (offset now: {})",
            stamp_sec,
            cursor.position()
        );

        // Read Header.stamp.nsec (uint32)
        let stamp_nsec = cursor.read_u32()?;
        println!(
            "Header.stamp.nsec = {} (offset now: {})",
            stamp_nsec,
            cursor.position()
        );

        // Read Header.frame_id length (uint32)
        let frame_id_len = cursor.read_u32()?;
        println!(
            "Header.frame_id length = {} (offset now: {})",
            frame_id_len,
            cursor.position()
        );

        // Read Header.frame_id string
        let frame_id_bytes = cursor.read_bytes(frame_id_len as usize)?;
        let frame_id = String::from_utf8_lossy(frame_id_bytes);
        println!(
            "Header.frame_id = '{}' (offset now: {})",
            frame_id,
            cursor.position()
        );

        // Read json_data length (uint32)
        let json_data_len = cursor.read_u32()?;
        println!(
            "json_data length = {} (offset now: {})",
            json_data_len,
            cursor.position()
        );

        // Read json_data string (partial)
        let json_data_bytes = cursor.read_bytes(json_data_len.min(50) as usize)?;
        let json_data = String::from_utf8_lossy(json_data_bytes);
        println!("json_data (partial) = '{}'", json_data);

        break;
    }

    Ok(())
}
