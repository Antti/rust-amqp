use std::io::{MemReader, MemWriter, IoResult, IoError, InvalidInput, OtherIoError};
use protocol::{Method, MethodFrame, ContentHeaderFrame};

#[deriving(Show, Clone, Eq, PartialEq, FromPrimitive)]
pub enum FrameType {
    METHOD = 1,
    HEADERS = 2,
    BODY  = 3,
    HEARTBEAT = 8
}

#[deriving(Show, Clone, Eq, PartialEq)]
pub struct Frame {
    pub frame_type: FrameType,
    pub channel: u16,
    pub payload: Vec<u8>
}

impl Frame {
    pub fn decode(reader: &mut Reader) -> IoResult<Frame> {
        let mut header = MemReader::new(try!(reader.read_exact(7)));
        let frame_type_id = try!(header.read_byte());
        let channel = try!(header.read_be_u16());
        let size = try!(header.read_be_u32());
        let payload = try!(reader.read_exact(size as uint));
        let frame_end = try!(reader.read_u8());
        if payload.len() as u32 != size {
            return Err(IoError{kind: InvalidInput, desc: "Payload didn't read the full size", detail: None});
        }
        if frame_end != 0xCE {
            return Err(IoError{kind: InvalidInput, desc: "Frame end wasn't right", detail: None});
        }
        let frame_type = match FromPrimitive::from_u8(frame_type_id){
            Some(ft) => ft,
            None => return Err(IoError{kind: OtherIoError, desc: "Unknown frame type", detail: None})
        };

        let frame = Frame { frame_type: frame_type, channel: channel, payload : payload };
        Ok(frame)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut writer = MemWriter::new();
        writer.write_u8(self.frame_type as u8).unwrap();
        writer.write_be_u16(self.channel).unwrap();
        writer.write_be_u32(self.payload.len() as u32).unwrap();
        writer.write(self.payload.as_slice()).unwrap();
        writer.write_u8(0xCE).unwrap();
        writer.unwrap()
    }
}

// We need this method, so we can match on class_id & method_id
pub fn decode_method_frame(frame: Frame) -> MethodFrame {
    if frame.frame_type != METHOD {
        fail!("Not a method frame");
    }
    let mut reader = MemReader::new(frame.payload);
    let class_id = reader.read_be_u16().unwrap();
    let method_id = reader.read_be_u16().unwrap();
    let arguments = reader.read_to_end().unwrap();
    MethodFrame{class_id: class_id, method_id: method_id, arguments: arguments}
}

pub fn encode_method_frame(method: &Method) -> Vec<u8> {
    let mut writer = MemWriter::new();
    writer.write_be_u16(method.class_id()).unwrap();
    writer.write_be_u16(method.id()).unwrap();
    writer.write(method.encode().as_slice()).unwrap();
    writer.unwrap()
}

pub fn decode_content_header_frame(frame: Frame) -> IoResult<ContentHeaderFrame> {
    let mut reader = MemReader::new(frame.payload.clone());
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

pub fn encode_content_header_frame(frame: &ContentHeaderFrame) -> Vec<u8> {
    let mut writer = MemWriter::new();
    writer.write_be_u16(frame.content_class).unwrap();
    writer.write_be_u16(frame.weight).unwrap(); //0 all the time for now
    writer.write_be_u64(frame.body_size).unwrap();
    writer.write_be_u16(frame.properties_flags).unwrap();
    writer.write(frame.properties.as_slice()).unwrap();
    writer.unwrap()
}


#[test]
fn test_encode_decode(){
    let frame = Frame{frame_type: METHOD, channel: 5, payload: vec!(1,2,3,4,5)};
    assert_eq!(frame, Frame::decode(&mut MemReader::new(frame.encode())).unwrap());
}

