use std::io::{MemReader, MemWriter, Seek, IoResult};
use std::collections::TreeMap;

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

pub fn new() -> Table {
    TreeMap::new()
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
        x => panic!("Unknown type {}", x)
    };
    Ok(entry)
}

fn write_table_entry(writer: &mut MemWriter, table_entry: &TableEntry) -> IoResult<()> {
    match table_entry {
        &Bool(val) => { try!(writer.write_u8(b't')); try!(writer.write_u8(val as u8)); },
        &ShortShortInt(val) => { try!(writer.write_u8(b'b')); try!(writer.write_i8(val)); },
        &ShortShortUint(val) => { try!(writer.write_u8(b'B')); try!(writer.write_u8(val)); },
        &ShortInt(val) => { try!(writer.write_u8(b'U')); try!(writer.write_be_i16(val)); },
        &ShortUint(val) => { try!(writer.write_u8(b'u')); try!(writer.write_be_u16(val)); },
        &LongInt(val) => { try!(writer.write_u8(b'I')); try!(writer.write_be_i32(val)); },
        &LongUint(val) => { try!(writer.write_u8(b'i')); try!(writer.write_be_u32(val)); },
        &LongLongInt(val) => { try!(writer.write_u8(b'L')); try!(writer.write_be_i64(val)); },
        &LongLongUint(val) => { try!(writer.write_u8(b'l')); try!(writer.write_be_u64(val)); },
        &Float(val) => { try!(writer.write_u8(b'f')); try!(writer.write_be_f32(val)); },
        &Double(val) => { try!(writer.write_u8(b'd')); try!(writer.write_be_f64(val)); },
        &DecimalValue(scale, value) => {
            try!(writer.write_u8(b'D'));
            try!(writer.write_u8(scale));
            try!(writer.write_be_u32(value));
        },
        // ShortString(str) => {
        //  try!(writer.write_u8(b's'));
        //  try!(writer.write_u8(str.len() as u8));
        //  try!(writer.write(str.as_bytes()));
        // },
        &LongString(ref str) => {
            try!(writer.write_u8(b'S'));
            try!(writer.write_be_u32(str.len() as u32));
            try!(writer.write(str.as_bytes()));
        },
        &FieldArray(ref arr) => {
            try!(writer.write_u8(b'A'));
            try!(writer.write_be_u32(arr.len() as u32));
            for item in arr.iter(){
                try!(write_table_entry(writer, item));
            }
        },
        &Timestamp(val) => { try!(writer.write_u8(b'T')); try!(writer.write_be_u64(val)) },
        &FieldTable(ref table) => {
            try!(writer.write_u8(b'F'));
            try!(encode_table(writer, table));
        },
        &Void => { try!(writer.write_u8(b'V')) }
    }
    Ok(())
}

pub fn decode_table(reader: &mut MemReader) -> IoResult<Table> {
    let mut table = new();
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

pub fn encode_table(writer: &mut Writer, table: &Table) -> IoResult<()> {
    let mut tmp_writer = MemWriter::new();
    for (field_name, table_entry) in table.iter() {
        try!(tmp_writer.write_u8(field_name.len() as u8));
        try!(tmp_writer.write(field_name.as_bytes()));
        try!(write_table_entry(&mut tmp_writer, table_entry));
    }
    let buffer = tmp_writer.unwrap();
    try!(writer.write_be_u32(buffer.len() as u32));
    try!(writer.write(buffer.as_slice()));
    Ok(())
}
