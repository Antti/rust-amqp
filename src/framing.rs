use amqp_error::{AMQPResult, AMQPError};
use std::io::{Read, Write, Cursor};
use std::iter;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use enum_primitive::FromPrimitive;

enum_from_primitive! {
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum FrameType {
    METHOD = 1,
    HEADERS = 2,
    BODY  = 3,
    HEARTBEAT = 8
}
}

impl Copy for FrameType {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Frame {
    pub frame_type: FrameType,
    pub channel: u16,
    pub payload: Vec<u8>
}

impl Frame {
    pub fn decode<T: Read>(reader: &mut T) -> AMQPResult<Frame> {
        // cast sized [u8] to unsized
        let mut header = &mut [0u8; 7] as &mut [u8];
        try!(reader.read(&mut header));
        // Make a &mut to &[u8]. &mut &[u8] implements `Read` trait.
        // `Read` works by changing a mutable reference to immutable slice,
        // as in with a basic pointer manipulation.
        let header = &mut (header as &[u8]) as &mut &[u8];
        let frame_type_id = try!(header.read_u8());
        let channel = try!(header.read_u16::<BigEndian>());
        let size = try!(header.read_u32::<BigEndian>()) as usize;
        // We need to use Vec because the size is not know in compile time.
        let mut payload: Vec<u8> = iter::repeat(0u8).take(size).collect();
        try!(reader.read(&mut payload));
        let frame_end = try!(reader.read_u8());
        if payload.len() != size {
            return Err(AMQPError::DecodeError("Cannot read a full frame payload"));
        }
        if frame_end != 0xCE {
            return Err(AMQPError::DecodeError("Frame didn't end with 0xCE"));
        }
        let frame_type = match FrameType::from_u8(frame_type_id){
            Some(ft) => ft,
            None => return Err(AMQPError::DecodeError("Unknown frame type"))
        };

        let frame = Frame { frame_type: frame_type, channel: channel, payload : payload };
        Ok(frame)
    }

    pub fn encode(&self) -> AMQPResult<Vec<u8>> {
        let mut writer = vec!();
        try!(writer.write_u8(self.frame_type as u8));
        try!(writer.write_u16::<BigEndian>(self.channel));
        try!(writer.write_u32::<BigEndian>(self.payload.len() as u32));
        try!(writer.write_all(&self.payload));
        try!(writer.write_u8(0xCE));
        Ok(writer)
    }
}

#[derive(Debug, Clone)]
pub struct ContentHeaderFrame {
    pub content_class: u16,
    pub weight: u16,
    pub body_size: u64,
    pub properties_flags: u16,
    pub properties: Vec<u8>
}

impl ContentHeaderFrame {
    pub fn decode(frame: Frame) -> AMQPResult<ContentHeaderFrame> {
        let mut reader = Cursor::new(frame.payload);
        let content_class = try!(reader.read_u16::<BigEndian>());
        let weight = try!(reader.read_u16::<BigEndian>()); //0 all the time for now
        let body_size = try!(reader.read_u64::<BigEndian>());
        let properties_flags = try!(reader.read_u16::<BigEndian>());
        let mut properties = vec!();
        try!(reader.read_to_end(&mut properties));
        Ok(ContentHeaderFrame {
            content_class: content_class, weight: weight, body_size: body_size,
            properties_flags: properties_flags, properties: properties
        })
    }

    pub fn encode(&self) -> AMQPResult<Vec<u8>> {
        let mut writer = vec!();
        try!(writer.write_u16::<BigEndian>(self.content_class));
        try!(writer.write_u16::<BigEndian>(self.weight)); //0 all the time for now
        try!(writer.write_u64::<BigEndian>(self.body_size));
        try!(writer.write_u16::<BigEndian>(self.properties_flags));
        try!(writer.write_all(&self.properties));
        Ok(writer)
    }
}

#[test]
fn test_encode_decode(){
    let frame = Frame{ frame_type: FrameType::METHOD, channel: 5, payload: vec!(1,2,3,4,5) };
    let frame_encoded = frame.encode().ok().unwrap();
    assert_eq!(frame, Frame::decode(&mut Cursor::new(frame_encoded)).ok().unwrap());
}
