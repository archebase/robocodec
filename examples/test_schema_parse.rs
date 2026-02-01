use robocodec::schema::parse_schema;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema_str = r#"std_msgs/Header header
string json_data
================================================================================
MSG: std_msgs/Header
# Standard metadata for higher-level stamped data types.
# This is generally used to communicate timestamped data 
# in a particular coordinate frame.
# 
# sequence ID: consecutively increasing ID 
uint32 seq
#Two-integer timestamp that is expressed as:
# * stamp.sec: seconds (stamp_secs) since epoch (in Python the variable is called 'secs')
# * stamp.nsec: nanoseconds since stamp_secs (in Python the variable is called 'nsecs')
# time-handling sugar is provided by the client library
time stamp
#Frame this data is associated with
string frame_id"#;

    let schema = parse_schema("realsense2_camera/Metadata", schema_str)?;

    println!("Schema name: {}", schema.name);
    println!("Schema package: {:?}", schema.package);
    println!("\nTypes in schema:");

    for (type_name, msg_type) in &schema.types {
        println!("\n  Type: {}", type_name);
        for field in &msg_type.fields {
            println!("    Field: {} : {:?}", field.name, field.type_name);
        }
    }

    Ok(())
}
