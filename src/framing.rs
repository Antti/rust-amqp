use std::io::{MemReader, MemWriter, Seek, IoResult, IoError, InvalidInput, OtherIoError};
use std::collections::TreeMap;

#[deriving(Show, Clone, Eq, PartialEq)]
pub enum FrameType {
    METHOD = 1,
    HEADERS = 2,
    BODY  = 3,
    HEARTBEAT = 8
}

//Integers and string lengths are always unsigned and held in network byte order
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
        let frame_end = try!(reader.read_exact(1));
        if payload.len() as u32 != size {
            return Err(IoError{kind: InvalidInput, desc: "Payload didn't read the full size", detail: None});
        }
        if frame_end[0] != 0xCE {
            return Err(IoError{kind: InvalidInput, desc: "Frame end wasn't right", detail: None});
        }
        let frame_type = match get_frame_type(frame_type_id){
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
        writer.write_u8(0xCE);
        writer.unwrap()
    }
}

#[deriving(Show, Clone)]
pub enum TableEntry {
    Bool(bool),
    ShortShortInt(i8),
    ShortShortUint(u8),
    ShortInt(i16),
    ShortUint(u16),
    LongInt(i32),
    LongUint(u32),
    LongLongInt(i64),
    LongLongUint(u64),
    Float(f32),
    Double(f64),
    DecimalValue(u8, u32),
    // ShortString(String),
    LongString(String),
    FieldArray(Vec<TableEntry>),
    Timestamp(u64),
    FieldTable(Table),
    Void
}

#[deriving(Show)]
pub type Table = TreeMap<String, TableEntry>;

pub trait Method {
    fn decode(data: Vec<u8>) -> Self;
    fn encode(&self) -> Vec<u8>;
    fn name(&self) -> &'static str;
    fn id(&self) -> u16;
    fn class_id(&self) -> u16;
}

pub trait Class {
    fn name(&self) -> &'static str;
    fn id(&self) -> u16;
}

pub fn get_frame_type(frame_type: u8) -> Option<FrameType> {
    match frame_type {
        1 => Some(METHOD),
        2 => Some(HEADERS),
        3 => Some(BODY),
        8 => Some(HEARTBEAT),
        _ => None
    }
}

pub fn decode_method_frame(frame: &Frame) -> (u16, u16, Vec<u8>) {
    let mut reader = MemReader::new(frame.payload.clone());
    let class_id = reader.read_be_u16().unwrap();
    let method_id = reader.read_be_u16().unwrap();
    let arguments = reader.read_to_end().unwrap();
    (class_id, method_id, arguments)
}

pub fn encode_method_frame(method: &Method) -> Vec<u8> {
    let mut writer = MemWriter::new();
    writer.write_be_u16(method.class_id()).unwrap();
    writer.write_be_u16(method.id()).unwrap();
    writer.write(method.encode().as_slice()).unwrap();
    writer.unwrap()
}
//                                                    class_id, weight, body_size, property flags, property list
pub fn decode_content_header_frame(frame: &Frame) -> (u16, u16, u64, u16, Vec<u8>) {
    let mut reader = MemReader::new(frame.payload.clone());
    let class_id = reader.read_be_u16().unwrap();
    let weight = reader.read_be_u16().unwrap();
    let body_size = reader.read_be_u64().unwrap();
    let property_flags = reader.read_be_u16().unwrap();
    let properties = reader.read_to_end().unwrap();
    (class_id, weight, body_size, property_flags, properties)
}

fn read_table_entry(reader: &mut MemReader) -> IoResult<TableEntry> {
    let entry = match try!(reader.read_u8()) {
        b't' => Bool(if try!(reader.read_u8()) == 0 {false}else{true} ),
        b'b' => ShortShortInt(try!(reader.read_i8())),
        b'B' => ShortShortUint(try!(reader.read_u8())),
        b'U' => ShortInt(try!(reader.read_be_i16())),
        b'u' => ShortUint(try!(reader.read_be_u16())),
        b'I' => LongInt(try!(reader.read_be_i32())),
        b'i' => LongUint(try!(reader.read_be_u32())),
        b'L' => LongLongInt(try!(reader.read_be_i64())),
        b'l' => LongLongUint(try!(reader.read_be_u64())),
        b'f' => Float(try!(reader.read_be_f32())),
        b'd' => Double(try!(reader.read_be_f64())),
        b'D' => DecimalValue(try!(reader.read_u8()), try!(reader.read_be_u32())),
        // b's' => {
        //  let size = try!(reader.read_u8()) as uint;
        //  let str = String::from_utf8_lossy(try!(reader.read_exact(size)).as_slice()).into_string();
        //  ShortString(str)
        // },
        b'S' => {
            let size = try!(reader.read_be_u32()) as uint;
            let str = String::from_utf8_lossy(try!(reader.read_exact(size)).as_slice()).into_string();
            LongString(str)
        },
        b'A' => {
            let number = try!(reader.read_be_u32());
            let arr = range(0,number).map(|_| read_table_entry(reader).unwrap()).collect(); //can't use try because of the closure
            FieldArray(arr)
        },
        b'T' => Timestamp(try!(reader.read_be_u64())),
        b'F' => FieldTable(try!(decode_table(reader))),
        b'V' => Void,
        x => fail!("Unknown type {}", x)
    };
    Ok(entry)
}

fn write_table_entry(writer: &mut MemWriter, table_entry: TableEntry) -> IoResult<()> {
    match table_entry {
        Bool(val) => { try!(writer.write_u8(b't')); try!(writer.write_u8(val as u8)); },
        ShortShortInt(val) => { try!(writer.write_u8(b'b')); try!(writer.write_i8(val)); },
        ShortShortUint(val) => { try!(writer.write_u8(b'B')); try!(writer.write_u8(val)); },
        ShortInt(val) => { try!(writer.write_u8(b'U')); try!(writer.write_be_i16(val)); },
        ShortUint(val) => { try!(writer.write_u8(b'u')); try!(writer.write_be_u16(val)); },
        LongInt(val) => { try!(writer.write_u8(b'I')); try!(writer.write_be_i32(val)); },
        LongUint(val) => { try!(writer.write_u8(b'i')); try!(writer.write_be_u32(val)); },
        LongLongInt(val) => { try!(writer.write_u8(b'L')); try!(writer.write_be_i64(val)); },
        LongLongUint(val) => { try!(writer.write_u8(b'l')); try!(writer.write_be_u64(val)); },
        Float(val) => { try!(writer.write_u8(b'f')); try!(writer.write_be_f32(val)); },
        Double(val) => { try!(writer.write_u8(b'd')); try!(writer.write_be_f64(val)); },
        DecimalValue(scale, value) => {
            try!(writer.write_u8(b'D'));
            try!(writer.write_u8(scale));
            try!(writer.write_be_u32(value));
        },
        // ShortString(str) => {
        //  try!(writer.write_u8(b's'));
        //  try!(writer.write_u8(str.len() as u8));
        //  try!(writer.write(str.as_bytes()));
        // },
        LongString(str) => {
            try!(writer.write_u8(b'S'));
            try!(writer.write_be_u32(str.len() as u32));
            try!(writer.write(str.as_bytes()));
        },
        FieldArray(arr) => {
            try!(writer.write_u8(b'A'));
            try!(writer.write_be_u32(arr.len() as u32));
            for item in arr.move_iter(){
                try!(write_table_entry(writer, item));
            }
        },
        Timestamp(val) => { try!(writer.write_u8(b'T')); try!(writer.write_be_u64(val)) },
        FieldTable(table) => {
            try!(writer.write_u8(b'F'));
            try!(encode_table(writer, table));
        },
        Void => { try!(writer.write_u8(b'V')) }
    }
    Ok(())
}

pub fn decode_table(reader: &mut MemReader) -> IoResult<Table> {
    let mut table = TreeMap::new();
    let size = try!(reader.read_be_u32()) as u64;
    let pos = try!(reader.tell());

    while try!(reader.tell()) < (pos + size) {
        let size = try!(reader.read_u8()) as uint;
        let field_name = try!(reader.read_exact(size));
        let table_entry = try!(read_table_entry(reader));
        table.insert(String::from_utf8_lossy(field_name.as_slice()).into_string(), table_entry);
    }
    Ok(table)
}

pub fn encode_table(writer: &mut MemWriter, table: Table) -> IoResult<()> {
    let mut tmp_writer = MemWriter::new();
    for (field_name, table_entry) in table.move_iter() {
        try!(tmp_writer.write_u8(field_name.len() as u8));
        try!(tmp_writer.write(field_name.as_bytes()));
        write_table_entry(&mut tmp_writer, table_entry);
    }
    let buffer = tmp_writer.unwrap();
    try!(writer.write_be_u32(buffer.len() as u32));
    try!(writer.write(buffer.as_slice()));
    Ok(())
}

#[test]
fn test_encode_decode(){
    assert_eq!(frame, Frame::decode(&mut MemReader::new(frame.encode())).unwrap());
}

