use std::old_io::MemReader;
use amqp_error::{AMQPResult, AMQPError};
use std::num::FromPrimitive;

#[derive(Debug, Clone, Eq, PartialEq, FromPrimitive)]
pub enum FrameType {
    METHOD = 1,
    HEADERS = 2,
    BODY  = 3,
    HEARTBEAT = 8
}

impl Copy for FrameType {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Frame {
    pub frame_type: FrameType,
    pub channel: u16,
    pub payload: Vec<u8>
}

impl Frame {
    pub fn decode(reader: &mut Reader) -> AMQPResult<Frame> {
        let mut header = MemReader::new(try!(reader.read_exact(7)));
        let frame_type_id = try!(header.read_byte());
        let channel = try!(header.read_be_u16());
        let size = try!(header.read_be_u32());
        let payload = try!(reader.read_exact(size as usize));
        let frame_end = try!(reader.read_u8());
        if payload.len() as u32 != size {
            return Err(AMQPError::DecodeError("Payload didn't read the full size"));
        }
        if frame_end != 0xCE {
            return Err(AMQPError::DecodeError("Frame end wasn't right"));
        }
        let frame_type = match FromPrimitive::from_u8(frame_type_id){
            Some(ft) => ft,
            None => return Err(AMQPError::DecodeError("Unknown frame type"))
        };

        let frame = Frame { frame_type: frame_type, channel: channel, payload : payload };
        Ok(frame)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut writer = vec!();
        writer.write_u8(self.frame_type as u8).unwrap();
        writer.write_be_u16(self.channel).unwrap();
        writer.write_be_u32(self.payload.len() as u32).unwrap();
        writer.write_all(&self.payload).unwrap();
        writer.write_u8(0xCE).unwrap();
        writer
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
        let mut reader = MemReader::new(frame.payload);
        let content_class = try!(reader.read_be_u16());
        let weight = try!(reader.read_be_u16()); //0 all the time for now
        let body_size = try!(reader.read_be_u64());
        let properties_flags = try!(reader.read_be_u16());
        let properties = try!(reader.read_to_end());
        Ok(ContentHeaderFrame {
            content_class: content_class, weight: weight, body_size: body_size,
            properties_flags: properties_flags, properties: properties
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut writer = vec!();
        writer.write_be_u16(self.content_class).unwrap();
        writer.write_be_u16(self.weight).unwrap(); //0 all the time for now
        writer.write_be_u64(self.body_size).unwrap();
        writer.write_be_u16(self.properties_flags).unwrap();
        writer.write_all(&self.properties).unwrap();
        writer
    }
}

#[test]
fn test_encode_decode(){
    let frame = Frame{ frame_type: FrameType::METHOD, channel: 5, payload: vec!(1,2,3,4,5) };
    assert_eq!(frame, Frame::decode(&mut frame.encode().as_slice()).ok().unwrap());
}