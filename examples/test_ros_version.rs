use robocodec::schema::RosVersion;

fn main() {
    let types = [
        "realsense2_camera/Metadata",
        "std_msgs/Header",
        "kuavo_msgs/sensorsData",
        "sensor_msgs/CompressedImage",
    ];

    for t in types {
        let version = RosVersion::from_type_name(t);
        println!("{}: {:?}", t, version);
    }
}
