use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bit_vec::BitVec;
use std::io::{self, Cursor, Read, Write};

use table::{Table, decode_table, encode_table};
use amqp_error::{AMQPError, AMQPResult};
use framing::{FrameType, Frame, MethodFrame};
use protocol;

pub struct ArgumentsReader<'data> {
    cursor: Cursor<&'data [u8]>,
    bits: BitVec,
    byte: u8,
    current_bit: u8
}

impl <'data> ArgumentsReader<'data> {
    fn new(data: &'data [u8]) -> Self {
        Self { cursor: Cursor::new(data), bits: BitVec::from_bytes(&[0]), byte: 0, current_bit: 0 }
    }

    fn read_octet(&mut self) -> AMQPResult<u8>  {
        self.cursor.read_u8().map_err(From::from)
    }

    fn read_long(&mut self) -> AMQPResult<u32>  {
        self.cursor.read_u32::<BigEndian>().map_err(From::from)
    }

    fn read_longlong(&mut self) -> AMQPResult<u64>  {
        self.cursor.read_u64::<BigEndian>().map_err(From::from)
    }

    fn read_short(&mut self) -> AMQPResult<u16>  {
        self.cursor.read_u16::<BigEndian>().map_err(From::from)
    }

    fn read_shortstr(&mut self) -> AMQPResult<String> {
        let size = self.read_octet()? as usize;
        let mut buffer: Vec<u8> = vec![0u8; size];
        self.cursor.read(&mut buffer[..])?;
        Ok(String::from_utf8_lossy(&buffer[..]).to_string())
    }

    fn read_longstr(&mut self) -> AMQPResult<String> {
        let size = self.read_long()? as usize;
        let mut buffer: Vec<u8> = vec![0u8; size];
        self.cursor.read(&mut buffer[..])?;
        Ok(String::from_utf8_lossy(&buffer[..]).to_string())
    }

    // Always a last method, since it reads to the end
    fn read_table(mut self) -> AMQPResult<Table> {
        decode_table(&mut self.cursor.into_inner())
    }

    fn read_timestamp(&mut self) -> AMQPResult<u64>  {
        self.read_longlong()
    }

    // TODO: Reset current_bit on all subsequent other type of data reads
    fn read_bit(&mut self) -> AMQPResult<bool> {
        if self.current_bit == 0 {
            self.byte = self.read_octet()?;
            self.bits = BitVec::from_bytes(&[self.byte]);
        }
        self.current_bit += 1;
        self.bits.get(8 - (self.current_bit - 1) as usize).ok_or(AMQPError::Protocol("Bitmap is not correct".to_owned()))
    }
}

pub struct ArgumentsWriter {
    data: Vec<u8>,
    bits: BitVec,
    current_bit: u8
}

impl ArgumentsWriter {
    fn new() -> Self {
        Self { data: vec![], bits: BitVec::from_bytes(&[0]), current_bit: 0 }
    }

    fn write_octet(&mut self, data: &u8) -> AMQPResult<()>  {
        self.data.write_u8(*data).map_err(From::from)
    }

    fn write_long(&mut self, data: &u32) -> AMQPResult<()>  {
        self.data.write_u32::<BigEndian>(*data).map_err(From::from)
    }

    fn write_longlong(&mut self, data: &u64) -> AMQPResult<()>  {
        self.data.write_u64::<BigEndian>(*data).map_err(From::from)
    }

    fn write_short(&mut self, data: &u16) -> AMQPResult<()>  {
        self.data.write_u16::<BigEndian>(*data).map_err(From::from)
    }

    fn write_shortstr(&mut self, data: &String) -> AMQPResult<()> {
        self.data.write_u8(data.len() as u8)?;
        self.data.write_all(data.as_bytes())?;
        Ok(())
    }

    fn write_longstr(&mut self, data: &String) -> AMQPResult<()> {
        self.data.write_u32::<BigEndian>(data.len() as u32)?;
        self.data.write_all(data.as_bytes())?;
        Ok(())
    }

    // Always a last method, since it reads to the end
    fn write_table(mut self, data: &Table) -> AMQPResult<()> {
        encode_table(&mut self.data,&data)
    }

    fn write_timestamp(&mut self, data: &u64) -> AMQPResult<()>  {
        self.write_longlong(data)
    }

    // TODO: Flush bytes on all subsequent other type of data writes
    fn write_bit(&mut self, data: &bool) -> AMQPResult<()> {
        println!("Setting bit: {} on a position {}", data, self.current_bit);
        self.current_bit += 1;
        self.bits.set(7 - (self.current_bit - 1) as usize, *data);
        if self.current_bit == 7 {
            self.flush_bits();
        }
        Ok(())
    }

    fn flush_bits(&mut self) -> AMQPResult<()> {
        if self.current_bit > 0 {
            let res = self.data.write_all(&self.bits.to_bytes()).map_err(From::from);
            self.bits = BitVec::from_bytes(&[0]);
            self.current_bit = 0;
            res
        } else {
            Ok(())
        }
    }

    fn as_bytes(mut self) -> Vec<u8> {
        self.flush_bits();
        self.data
    }
}


macro_rules! map_type {
    (octet) => (u8);
    (long) => (u32);
    (longlong) => (u64);
    (short) => (u16);
    (shortstr) => (String);
    (longstr) => (String);
    (table) => (Table);
    (timestamp) => (u64);
    (bit) => (bool);
}

macro_rules! read_type {
    ($reader:expr, octet) => ($reader.read_octet());
    ($reader:expr, long) => ($reader.read_long());
    ($reader:expr, longlong) => ($reader.read_longlong());
    ($reader:expr, short) => ($reader.read_short());
    ($reader:expr, shortstr) => ($reader.read_shortstr());
    ($reader:expr, longstr) => ($reader.read_longstr());
    ($reader:expr, table) => ($reader.read_table());
    ($reader:expr, timestamp) => ($reader.read_timestamp());
    ($reader:expr, bit) => ($reader.read_bit());
}

macro_rules! write_type {
    ($writer:expr, octet, $data:expr) => ($writer.write_octet($data));
    ($writer:expr, long, $data:expr) => ($writer.write_long($data));
    ($writer:expr, longlong, $data:expr) => ($writer.write_longlong($data));
    ($writer:expr, short, $data:expr) => ($writer.write_short($data));
    ($writer:expr, shortstr, $data:expr) => ($writer.write_shortstr($data));
    ($writer:expr, longstr, $data:expr) => ($writer.write_longstr($data));
    ($writer:expr, table, $data:expr) => ($writer.write_table($data));
    ($writer:expr, timestamp, $data:expr) => ($writer.write_timestamp($data));
    ($writer:expr, bit, $data:expr) => ($writer.write_bit($data));
}

macro_rules! method_struct {
    ($method_name:ident, $method_str:expr, $method_id:expr, $class_id:expr, $($arg_name:ident => $ty:ident),+) => (
        struct $method_name {
            $($arg_name: map_type!($ty),)*
        }

        impl protocol::Method for $method_name {
            fn decode(method_frame: MethodFrame) -> AMQPResult<Self> where Self: Sized {
                let mut reader = ArgumentsReader::new(&method_frame.arguments);
                Ok($method_name {
                    $($arg_name: read_type!(reader, $ty)?,)*
                })
            }

            fn encode(&self) -> AMQPResult<Vec<u8>> {
                let mut writer = ArgumentsWriter::new();
                $(write_type!(writer, $ty, &self.$arg_name)?;)*
                Ok(writer.as_bytes())
            }

            fn name(&self) -> &'static str {
                $method_str
            }

            fn id(&self) -> u16 {
                $method_id
            }

            fn class_id(&self) -> u16 {
                $class_id
            }
        }
    )
}

#[cfg(test)]
mod test {
    use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
    use bit_vec::BitVec;
    use std::io::{self, Cursor, Read, Write};

    use table::{Table, decode_table, encode_table};
    use amqp_error::{AMQPError, AMQPResult};
    use framing::{FrameType, Frame, MethodFrame};
    use protocol;
    use super::*;

    method_struct!(Foo, "test.foo", 1, 2, a => octet, b => shortstr, c => longstr, d => bit, e => bit, f => long);

    #[test]
    fn test_foo(){
        use protocol::Method;
        let f = Foo { a: 1, b: "test".to_string(), c: "bar".to_string(), d: false, e: true, f: 99222456 };
        assert_eq!(f.encode().unwrap(), vec![
            1, //a
            4, // "test".len()
            116, 101, 115, 116, // "test"
            0, 0, 0, 3, // "bar".len()
            98, 97, 114, // "bar"
            2 // false, true => 0b00000010
        ]);
    }
}
