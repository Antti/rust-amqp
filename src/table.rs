use std::collections::HashMap;
use amqp_error::{AMQPError, AMQPResult};
use std::io::{Read, Write};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::iter;

#[derive(Debug, Clone)]
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

pub type Table = HashMap<String, TableEntry>;

pub fn new() -> Table {
    HashMap::new()
}

fn read_table_entry(reader: &mut &[u8]) -> AMQPResult<TableEntry> {
    let entry = match try!(reader.read_u8()) {
        b't' => TableEntry::Bool(if try!(reader.read_u8()) == 0 {false}else{true} ),
        b'b' => TableEntry::ShortShortInt(try!(reader.read_i8())),
        b'B' => TableEntry::ShortShortUint(try!(reader.read_u8())),
        b'U' => TableEntry::ShortInt(try!(reader.read_i16::<BigEndian>())),
        b'u' => TableEntry::ShortUint(try!(reader.read_u16::<BigEndian>())),
        b'I' => TableEntry::LongInt(try!(reader.read_i32::<BigEndian>())),
        b'i' => TableEntry::LongUint(try!(reader.read_u32::<BigEndian>())),
        b'L' => TableEntry::LongLongInt(try!(reader.read_i64::<BigEndian>())),
        b'l' => TableEntry::LongLongUint(try!(reader.read_u64::<BigEndian>())),
        b'f' => TableEntry::Float(try!(reader.read_f32::<BigEndian>())),
        b'd' => TableEntry::Double(try!(reader.read_f64::<BigEndian>())),
        b'D' => TableEntry::DecimalValue(try!(reader.read_u8()), try!(reader.read_u32::<BigEndian>())),
        // b's' => {
        //  let size = try!(reader.read_u8()) as usize;
        //  let str = String::from_utf8_lossy(try!(reader.read_exact(size)).as_slice()).to_string();
        //  ShortString(str)
        // },
        b'S' => {
            let size = try!(reader.read_u32::<BigEndian>()) as usize;
            let mut buffer: Vec<u8> = iter::repeat(0u8).take(size).collect();
            try!(reader.read(&mut buffer[..]));
            let str = String::from_utf8_lossy(&buffer).to_string();
            TableEntry::LongString(str)
        },
        b'A' => {
            let number = try!(reader.read_u32::<BigEndian>());
            let mut arr = Vec::with_capacity(number as usize);
            for _ in (0..number) {
                arr.push(try!(read_table_entry(reader)))
            }
            TableEntry::FieldArray(arr)
        },
        b'T' => TableEntry::Timestamp(try!(reader.read_u64::<BigEndian>())),
        b'F' => TableEntry::FieldTable(try!(decode_table(reader))),
        b'V' => TableEntry::Void,
        x => { debug!("Unknown type: {}", x); return Err(AMQPError::DecodeError("Unknown type")) },
    };
    Ok(entry)
}

fn write_table_entry(writer: &mut Vec<u8>, table_entry: &TableEntry) -> AMQPResult<()> {
    match table_entry {
        &TableEntry::Bool(val) => { try!(writer.write_u8(b't')); try!(writer.write_u8(val as u8)); },
        &TableEntry::ShortShortInt(val) => { try!(writer.write_u8(b'b')); try!(writer.write_i8(val)); },
        &TableEntry::ShortShortUint(val) => { try!(writer.write_u8(b'B')); try!(writer.write_u8(val)); },
        &TableEntry::ShortInt(val) => { try!(writer.write_u8(b'U')); try!(writer.write_i16::<BigEndian>(val)); },
        &TableEntry::ShortUint(val) => { try!(writer.write_u8(b'u')); try!(writer.write_u16::<BigEndian>(val)); },
        &TableEntry::LongInt(val) => { try!(writer.write_u8(b'I')); try!(writer.write_i32::<BigEndian>(val)); },
        &TableEntry::LongUint(val) => { try!(writer.write_u8(b'i')); try!(writer.write_u32::<BigEndian>(val)); },
        &TableEntry::LongLongInt(val) => { try!(writer.write_u8(b'L')); try!(writer.write_i64::<BigEndian>(val)); },
        &TableEntry::LongLongUint(val) => { try!(writer.write_u8(b'l')); try!(writer.write_u64::<BigEndian>(val)); },
        &TableEntry::Float(val) => { try!(writer.write_u8(b'f')); try!(writer.write_f32::<BigEndian>(val)); },
        &TableEntry::Double(val) => { try!(writer.write_u8(b'd')); try!(writer.write_f64::<BigEndian>(val)); },
        &TableEntry::DecimalValue(scale, value) => {
            try!(writer.write_u8(b'D'));
            try!(writer.write_u8(scale));
            try!(writer.write_u32::<BigEndian>(value));
        },
        // ShortString(str) => {
        //  try!(writer.write_u8(b's'));
        //  try!(writer.write_u8(str.len() as u8));
        //  try!(writer.write_all(str.as_bytes()));
        // },
        &TableEntry::LongString(ref str) => {
            try!(writer.write_u8(b'S'));
            try!(writer.write_u32::<BigEndian>(str.len() as u32));
            try!(writer.write_all(str.as_bytes()));
        },
        &TableEntry::FieldArray(ref arr) => {
            try!(writer.write_u8(b'A'));
            try!(writer.write_u32::<BigEndian>(arr.len() as u32));
            for item in arr.iter(){
                try!(write_table_entry(writer, item));
            }
        },
        &TableEntry::Timestamp(val) => { try!(writer.write_u8(b'T')); try!(writer.write_u64::<BigEndian>(val)) },
        &TableEntry::FieldTable(ref table) => {
            try!(writer.write_u8(b'F'));
            try!(encode_table(writer, table));
        },
        &TableEntry::Void => { try!(writer.write_u8(b'V')) }
    }
    Ok(())
}

pub fn decode_table(reader: &mut &[u8]) -> AMQPResult<Table> {
    debug!("decoding table");
    let mut table = new();
    let size = try!(reader.read_u32::<BigEndian>()) as usize;
    let total_len = reader.len();

    while reader.len() > total_len - size {
        let field_name_size = try!(reader.read_u8()) as usize;
        let mut field_name: Vec<u8> = iter::repeat(0u8).take(field_name_size).collect();
        try!(reader.read(&mut field_name[..]));
        let table_entry = try!(read_table_entry(reader));
        debug!("Read table entry: {:?} = {:?}", field_name, table_entry);
        table.insert(String::from_utf8_lossy(&field_name).to_string(), table_entry);
    }
    Ok(table)
}

pub fn encode_table<T: Write>(writer: &mut T, table: &Table) -> AMQPResult<()> {
    let mut tmp_buffer = vec!();
    for (field_name, table_entry) in table.iter() {
        try!(tmp_buffer.write_u8(field_name.len() as u8));
        try!(tmp_buffer.write_all(field_name.as_bytes()));
        try!(write_table_entry(&mut tmp_buffer, table_entry));
    }
    try!(writer.write_u32::<BigEndian>(tmp_buffer.len() as u32));
    try!(writer.write_all(&tmp_buffer));
    Ok(())
}
