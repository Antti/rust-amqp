use collect::tree_map::TreeMap;
use amqp_error::AMQPResult;

#[derive(Show, Clone)]
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

pub type Table = TreeMap<String, TableEntry>;

pub fn new() -> Table {
    TreeMap::new()
}

fn read_table_entry(reader: &mut &[u8]) -> AMQPResult<TableEntry> {
    let entry = match try!(reader.read_u8()) {
        b't' => TableEntry::Bool(if try!(reader.read_u8()) == 0 {false}else{true} ),
        b'b' => TableEntry::ShortShortInt(try!(reader.read_i8())),
        b'B' => TableEntry::ShortShortUint(try!(reader.read_u8())),
        b'U' => TableEntry::ShortInt(try!(reader.read_be_i16())),
        b'u' => TableEntry::ShortUint(try!(reader.read_be_u16())),
        b'I' => TableEntry::LongInt(try!(reader.read_be_i32())),
        b'i' => TableEntry::LongUint(try!(reader.read_be_u32())),
        b'L' => TableEntry::LongLongInt(try!(reader.read_be_i64())),
        b'l' => TableEntry::LongLongUint(try!(reader.read_be_u64())),
        b'f' => TableEntry::Float(try!(reader.read_be_f32())),
        b'd' => TableEntry::Double(try!(reader.read_be_f64())),
        b'D' => TableEntry::DecimalValue(try!(reader.read_u8()), try!(reader.read_be_u32())),
        // b's' => {
        //  let size = try!(reader.read_u8()) as usize;
        //  let str = String::from_utf8_lossy(try!(reader.read_exact(size)).as_slice()).to_string();
        //  ShortString(str)
        // },
        b'S' => {
            let size = try!(reader.read_be_u32()) as usize;
            let str = String::from_utf8_lossy(&try!(reader.read_exact(size))[]).to_string();
            TableEntry::LongString(str)
        },
        b'A' => {
            let number = try!(reader.read_be_u32());
            let arr = range(0,number).map(|_| read_table_entry(reader).ok().unwrap()).collect(); //can't use try because of the closure
            TableEntry::FieldArray(arr)
        },
        b'T' => TableEntry::Timestamp(try!(reader.read_be_u64())),
        b'F' => TableEntry::FieldTable(try!(decode_table(reader))),
        b'V' => TableEntry::Void,
        x => panic!("Unknown type {}", x)
    };
    Ok(entry)
}

fn write_table_entry(writer: &mut Vec<u8>, table_entry: &TableEntry) -> AMQPResult<()> {
    match table_entry {
        &TableEntry::Bool(val) => { try!(writer.write_u8(b't')); try!(writer.write_u8(val as u8)); },
        &TableEntry::ShortShortInt(val) => { try!(writer.write_u8(b'b')); try!(writer.write_i8(val)); },
        &TableEntry::ShortShortUint(val) => { try!(writer.write_u8(b'B')); try!(writer.write_u8(val)); },
        &TableEntry::ShortInt(val) => { try!(writer.write_u8(b'U')); try!(writer.write_be_i16(val)); },
        &TableEntry::ShortUint(val) => { try!(writer.write_u8(b'u')); try!(writer.write_be_u16(val)); },
        &TableEntry::LongInt(val) => { try!(writer.write_u8(b'I')); try!(writer.write_be_i32(val)); },
        &TableEntry::LongUint(val) => { try!(writer.write_u8(b'i')); try!(writer.write_be_u32(val)); },
        &TableEntry::LongLongInt(val) => { try!(writer.write_u8(b'L')); try!(writer.write_be_i64(val)); },
        &TableEntry::LongLongUint(val) => { try!(writer.write_u8(b'l')); try!(writer.write_be_u64(val)); },
        &TableEntry::Float(val) => { try!(writer.write_u8(b'f')); try!(writer.write_be_f32(val)); },
        &TableEntry::Double(val) => { try!(writer.write_u8(b'd')); try!(writer.write_be_f64(val)); },
        &TableEntry::DecimalValue(scale, value) => {
            try!(writer.write_u8(b'D'));
            try!(writer.write_u8(scale));
            try!(writer.write_be_u32(value));
        },
        // ShortString(str) => {
        //  try!(writer.write_u8(b's'));
        //  try!(writer.write_u8(str.len() as u8));
        //  try!(writer.write(str.as_bytes()));
        // },
        &TableEntry::LongString(ref str) => {
            try!(writer.write_u8(b'S'));
            try!(writer.write_be_u32(str.len() as u32));
            try!(writer.write(str.as_bytes()));
        },
        &TableEntry::FieldArray(ref arr) => {
            try!(writer.write_u8(b'A'));
            try!(writer.write_be_u32(arr.len() as u32));
            for item in arr.iter(){
                try!(write_table_entry(writer, item));
            }
        },
        &TableEntry::Timestamp(val) => { try!(writer.write_u8(b'T')); try!(writer.write_be_u64(val)) },
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
    let size = try!(reader.read_be_u32()) as usize;
    let total_len = reader.len();

    while reader.len() > total_len - size {
        let field_name_size = try!(reader.read_u8()) as usize;
        let field_name = try!(reader.read_exact(field_name_size));
        let table_entry = try!(read_table_entry(reader));
        debug!("Read table entry: {:?} = {:?}", field_name, table_entry);
        table.insert(String::from_utf8_lossy(&field_name[]).to_string(), table_entry);
    }
    Ok(table)
}

pub fn encode_table(writer: &mut Writer, table: &Table) -> AMQPResult<()> {
    let mut tmp_buffer = vec!();
    for (field_name, table_entry) in table.iter() {
        try!(tmp_buffer.write_u8(field_name.len() as u8));
        try!(tmp_buffer.write(field_name.as_bytes()));
        try!(write_table_entry(&mut tmp_buffer, table_entry));
    }
    try!(writer.write_be_u32(tmp_buffer.len() as u32));
    try!(writer.write(&tmp_buffer[]));
    Ok(())
}
