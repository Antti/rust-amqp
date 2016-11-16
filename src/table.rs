use std::collections::HashMap;
use amqp_error::{AMQPError, AMQPResult};
use std::io::{Read, Write};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

#[derive(Debug, Clone, PartialEq)]
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
    Void,
}

pub type Table = HashMap<String, TableEntry>;

pub trait Init {
    fn new() -> Self;
}

impl Init for Table {
    fn new() -> Self {
        HashMap::new()
    }
}

fn read_table_entry<T>(reader: &mut T) -> AMQPResult<(TableEntry, usize)> where T: Read {
    let (entry, entry_size) = match try!(reader.read_u8()) {
        b't' => (TableEntry::Bool(!try!(reader.read_u8()) != 0), 1),
        b'b' => (TableEntry::ShortShortInt(try!(reader.read_i8())), 1),
        b'B' => (TableEntry::ShortShortUint(try!(reader.read_u8())), 1),
        b'U' => (TableEntry::ShortInt(try!(reader.read_i16::<BigEndian>())), 2),
        b'u' => (TableEntry::ShortUint(try!(reader.read_u16::<BigEndian>())), 2),
        b'I' => (TableEntry::LongInt(try!(reader.read_i32::<BigEndian>())), 4),
        b'i' => (TableEntry::LongUint(try!(reader.read_u32::<BigEndian>())), 4),
        b'L' => (TableEntry::LongLongInt(try!(reader.read_i64::<BigEndian>())), 8),
        b'l' => (TableEntry::LongLongUint(try!(reader.read_u64::<BigEndian>())), 8),
        b'f' => (TableEntry::Float(try!(reader.read_f32::<BigEndian>())), 4),
        b'd' => (TableEntry::Double(try!(reader.read_f64::<BigEndian>())), 8),
        b'D' => ({
            TableEntry::DecimalValue(try!(reader.read_u8()), try!(reader.read_u32::<BigEndian>()))
        }, 5),
        // b's' => {
        //  let size = try!(reader.read_u8()) as usize;
        // let str =
        // String::from_utf8_lossy(try!(reader.read_exact(size)).as_slice()).to_string();
        //  ShortString(str)
        // },
        b'S' => {
            let size = try!(reader.read_u32::<BigEndian>()) as usize;
            let mut buffer: Vec<u8> = vec![0u8; size];
            try!(reader.read(&mut buffer[..]));
            let string = String::from_utf8_lossy(&buffer).to_string();
            let entry = TableEntry::LongString(string);
            (entry, 4 + size)
        },
        b'A' => {
            let array_len = try!(reader.read_u32::<BigEndian>()) as usize;
            let mut read_len = 0;
            let mut arr = Vec::new();
            while read_len < array_len {
                let (entry, entry_len) = try!(read_table_entry(reader));
                read_len += entry_len;
                arr.push(entry)
            }
            let entry = TableEntry::FieldArray(arr);
            (entry, 4 + array_len)
        },
        b'T' => (TableEntry::Timestamp(try!(reader.read_u64::<BigEndian>())), 8),
        b'F' => {
            let (table, table_size) = try!(decode_table(reader));
            let entry = TableEntry::FieldTable(table);
            (entry, table_size)
        },
        b'V' => (TableEntry::Void, 0),
        x => {
            debug!("Unknown type: {}", x);
            return Err(AMQPError::DecodeError("Unknown type"));
        }
    };
    Ok((entry, entry_size + 1)) // including entry_type
}

fn write_table_entry(writer: &mut Vec<u8>, table_entry: &TableEntry) -> AMQPResult<()> {
    match *table_entry {
        TableEntry::Bool(val) => {
            try!(writer.write_u8(b't'));
            try!(writer.write_u8(val as u8));
        }
        TableEntry::ShortShortInt(val) => {
            try!(writer.write_u8(b'b'));
            try!(writer.write_i8(val));
        }
        TableEntry::ShortShortUint(val) => {
            try!(writer.write_u8(b'B'));
            try!(writer.write_u8(val));
        }
        TableEntry::ShortInt(val) => {
            try!(writer.write_u8(b'U'));
            try!(writer.write_i16::<BigEndian>(val));
        }
        TableEntry::ShortUint(val) => {
            try!(writer.write_u8(b'u'));
            try!(writer.write_u16::<BigEndian>(val));
        }
        TableEntry::LongInt(val) => {
            try!(writer.write_u8(b'I'));
            try!(writer.write_i32::<BigEndian>(val));
        }
        TableEntry::LongUint(val) => {
            try!(writer.write_u8(b'i'));
            try!(writer.write_u32::<BigEndian>(val));
        }
        TableEntry::LongLongInt(val) => {
            try!(writer.write_u8(b'L'));
            try!(writer.write_i64::<BigEndian>(val));
        }
        TableEntry::LongLongUint(val) => {
            try!(writer.write_u8(b'l'));
            try!(writer.write_u64::<BigEndian>(val));
        }
        TableEntry::Float(val) => {
            try!(writer.write_u8(b'f'));
            try!(writer.write_f32::<BigEndian>(val));
        }
        TableEntry::Double(val) => {
            try!(writer.write_u8(b'd'));
            try!(writer.write_f64::<BigEndian>(val));
        }
        TableEntry::DecimalValue(scale, value) => {
            try!(writer.write_u8(b'D'));
            try!(writer.write_u8(scale));
            try!(writer.write_u32::<BigEndian>(value));
        }
        // ShortString(str) => {
        //  try!(writer.write_u8(b's'));
        //  try!(writer.write_u8(str.len() as u8));
        //  try!(writer.write_all(str.as_bytes()));
        // },
        TableEntry::LongString(ref str) => {
            try!(writer.write_u8(b'S'));
            try!(writer.write_u32::<BigEndian>(str.len() as u32));
            try!(writer.write_all(str.as_bytes()));
        }
        TableEntry::FieldArray(ref arr) => {
            try!(writer.write_u8(b'A'));
            let mut tmp_buffer = vec![];
            for item in arr.iter() {
                try!(write_table_entry(&mut tmp_buffer, item));
            }
            try!(writer.write_u32::<BigEndian>(tmp_buffer.len() as u32));
            try!(writer.write(&tmp_buffer));
        }
        TableEntry::Timestamp(val) => {
            try!(writer.write_u8(b'T'));
            try!(writer.write_u64::<BigEndian>(val))
        }
        TableEntry::FieldTable(ref table) => {
            try!(writer.write_u8(b'F'));
            try!(encode_table(writer, table));
        }
        TableEntry::Void => try!(writer.write_u8(b'V')),
    }
    Ok(())
}

pub fn decode_table<T>(reader: &mut T) -> AMQPResult<(Table, usize)> where T: Read {
    let mut table = Table::new();
    let table_len = try!(reader.read_u32::<BigEndian>()) as usize;
    debug!("decoding table, len: {}", table_len);
    let mut bytes_read = 0;

    while bytes_read < table_len {
        let field_name_len = try!(reader.read_u8()) as usize;
        let mut field_name: Vec<u8> = vec![0u8; field_name_len];
        try!(reader.read(&mut field_name[..]));
        let (table_entry, table_entry_size) = try!(read_table_entry(reader));
        let stringified_field_name = String::from_utf8_lossy(&field_name).to_string();
        debug!("Read table entry: {:?}:{} = {:?}", stringified_field_name, table_entry_size, table_entry);
        table.insert(stringified_field_name, table_entry);
        bytes_read += 1 + field_name_len + table_entry_size; // a byte for length of the field_name
        debug!("bytes_read: {} of {}", bytes_read, table_len);
    }
    debug!("table decoded, table len: {}, bytes_read: {}", table_len, bytes_read);
    Ok((table, bytes_read + 4)) // 4 bytes for the table_len
}

pub fn encode_table<T: Write>(writer: &mut T, table: &Table) -> AMQPResult<()> {
    let mut tmp_buffer = vec![];
    for (field_name, table_entry) in table.iter() {
        try!(tmp_buffer.write_u8(field_name.len() as u8));
        try!(tmp_buffer.write_all(field_name.as_bytes()));
        try!(write_table_entry(&mut tmp_buffer, table_entry));
    }
    try!(writer.write_u32::<BigEndian>(tmp_buffer.len() as u32));
    try!(writer.write_all(&tmp_buffer));
    Ok(())
}
