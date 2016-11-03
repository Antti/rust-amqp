gem 'json'
require 'json'
require 'erb'

# def pad_str(count, str)
#   str.lines.map{|line| "#{" "*count}#{line.lstrip}" }.join()
# end

def read_type(type)
  case type
  when "octet"
    "try!(reader.read_u8())"
  when "long"
    "try!(reader.read_u32::<BigEndian>())"
  when "longlong"
    "try!(reader.read_u64::<BigEndian>())"
  when "short"
    "try!(reader.read_u16::<BigEndian>())"
  when "bit"
    raise "Cant read bit here..."
  when "shortstr"
    "{
          let size = try!(reader.read_u8()) as usize;
          let mut buffer: Vec<u8> = vec![0u8; size];
          try!(reader.read(&mut buffer[..]));
          String::from_utf8_lossy(&buffer[..]).to_string()
     }"
  when "longstr"
    "{
          let size = try!(reader.read_u32::<BigEndian>()) as usize;
          let mut buffer: Vec<u8> = vec![0u8; size];
          try!(reader.read(&mut buffer[..]));
          String::from_utf8_lossy(&buffer[..]).to_string()
      }"
  when "table"
    "try!(decode_table(reader))"
  when "timestamp"
    "try!(reader.read_u64::<BigEndian>())"
  else
    raise "Unknown type: #{type}"
  end
end

def write_type(name, type)
  case type
  when "octet"
    "try!(writer.write_u8(#{name}));"
  when "long"
    "try!(writer.write_u32::<BigEndian>(#{name}));"
  when "longlong"
    "try!(writer.write_u64::<BigEndian>(#{name}));"
  when "short"
    "try!(writer.write_u16::<BigEndian>(#{name}));"
  when "bit"
    raise "Cant write bit here..."
  when "shortstr"
    "try!(writer.write_u8(#{name}.len() as u8));
    try!(writer.write_all(#{name}.as_bytes()));"
  when "longstr"
    "try!(writer.write_u32::<BigEndian>(#{name}.len() as u32));
    try!(writer.write_all(#{name}.as_bytes()));"
  when "table"
    "try!(encode_table(&mut writer, &#{name}));"
  when "timestamp"
    "try!(writer.write_u64::<BigEndian>(#{name}));"
  else
    raise "Unknown type: #{type}"
  end
end

def generate_reader_body(arguments)
    body = []
    body << "let reader = &mut &method_frame.arguments[..];"
    n_bits = 0
    arguments.each do |argument|
      type = argument["domain"] ? map_domain(argument["domain"]) : argument["type"]
      if type == "bit"
        if n_bits == 0
          body << "let byte = try!(reader.read_u8());"
          body << "let bits = BitVec::from_bytes(&[byte]);"
        end
        body << "let #{snake_name(argument["name"])} = match bits.get(#{7-n_bits}){
          Some(bit) => bit,
          None => return Err(AMQPError::Protocol(\"Bitmap is not correct\".to_owned()))
        };"
        n_bits += 1
        if n_bits == 8
          n_bits = 0
        end
      else
        n_bits = 0
        body << "let #{snake_name(argument["name"])} = #{read_type(type)};"
      end
    end
    body
end

def generate_writer_body(arguments)
    body = []
    body << "let mut writer = vec![];"
    n_bits = 0
    arguments.each do |argument|
      type = argument["domain"] ? map_domain(argument["domain"]) : argument["type"]
      if type == "bit"
        if n_bits == 0
          body << "let mut bits = BitVec::from_elem(8, false);"
        end
        body << "bits.set(#{7-n_bits}, self.#{snake_name(argument["name"])});"
        n_bits += 1
        if n_bits == 8
          body << "try!(writer.write_all(&bits.to_bytes()));"
          n_bits = 0
        end
      else
        if n_bits > 0
          body << "try!(writer.write_all(&bits.to_bytes()));"
          n_bits = 0
        end
        body << write_type("self."+snake_name(argument["name"]), type)
      end
    end
    body << "try!(writer.write_all(&bits.to_bytes()));" if n_bits > 0 #if bits were the last element
    body << "Ok(writer)"
    body
end

spec_file = 'amqp-rabbitmq-0.9.1.json'
SPEC = JSON.load(File.read(spec_file))
DOMAINS = Hash[SPEC["domains"]]

class SpecGenerator
  def initialize(spec)
    @spec = spec
    modify_spec
  end

  def classes
    @spec["classes"]
  end

  def class_id_and_method_id_to_name
    @spec["classes"].flat_map do |klass|
      klass["methods"].map do |method|
        "(#{klass["id"]}, #{method["id"]}) => \"#{klass["name"]}.#{method["name"]}\""
      end
    end
  end

  def class_id_and_method_id_carries_content
    @spec["classes"].flat_map do |klass|
      klass["methods"].select{|m| m["content"] }.map do |method|
        "(#{klass["id"]}, #{method["id"]}) => #{!!method["content"]}"
      end
    end
  end


  def get_binding
    binding
  end

  private
  def modify_spec
    @spec["classes"].each do |klass|
      klass["struct_name"] = titleize(klass["name"])
      if klass["properties"] && klass["properties"].any?
        klass["properties_struct_name"] = "#{klass["struct_name"]}Properties"
        klass["properties_fields"] = klass["properties"].map do |prop|
          rust_type = map_type_to_rust prop["domain"] ? map_domain(prop["domain"]) : prop["type"]
          "pub #{snake_name prop["name"]}: Option<#{rust_type}>"
        end
        klass["properties_struct_create"] = klass["properties"].map{|arg| "#{snake_name arg["name"]}: #{snake_name arg["name"]}"}
        klass["properties"].each do |prop|
          prop["prop_name"] = snake_name prop["name"]
          prop["prop_type"] = prop["domain"] ? map_domain(prop["domain"]) : prop["type"]
        end
      end
      klass["methods"].each do |method|
        method["method_name"] = camel_name titleize(method["name"])
        method["method_struct_create"] = method["arguments"].map{|arg| "#{snake_name arg["name"]}: #{snake_name arg["name"]}"}
        method["fields"]= method["arguments"].map do |argument|
          rust_type = map_type_to_rust argument["domain"] ? map_domain(argument["domain"]) : argument["type"]
          "pub #{snake_name argument["name"]}: #{rust_type}"
        end
        method["readers"] = generate_reader_body(method["arguments"])
        method["writers"] = generate_writer_body(method["arguments"])
      end
    end
  end #modify_spec

  def value_to_rust_value(value)
    case value
    when String
      "\"#{value}\".to_owned()"
    when Fixnum
      value
    when TrueClass, FalseClass
      value
    when Hash
      "Table::new()"
    else
      raise "Cant convert value #{value}"
    end
  end

  def args_list(properties)
    properties.map do |prop|
      rust_type = map_type_to_rust prop["domain"] ? map_domain(prop["domain"]) : prop["type"]
      "#{snake_name(prop["name"])}: #{rust_type}"
    end
  end

  def titleize(name)
    name[0].upcase+name[1..-1]
  end

  def snake_name(name)
    name.tr("-","_").gsub(/^type$/, "_type")
  end

  def camel_name(klass)
    klass.gsub(/(\-.)/){|c| c[1].upcase}
  end

  def map_domain(domain)
    DOMAINS[domain]
  end

  def map_type_to_rust(type)
    case type
    when "octet"
      "u8"
    when "long"
      "u32"
    when "longlong"
      "u64"
    when "short"
      "u16"
    when "bit"
      'bool'
    when "shortstr"
      # 'Vec<u8>'
      String
    when "longstr"
      # 'Vec<u8>'
      String
    when "table"
      "Table"
    when "timestamp"
      "u64"
    else
      raise "Uknown type: #{type}"
    end
  end
end


method_frame_methods = <<-EOF
fn method_name(method_frame: &MethodFrame) -> &'static str {
    match (method_frame.class_id, method_frame.method_id) {
    <% class_id_and_method_id_to_name.each do |m| -%>
    <%= m %>,
    <% end -%>
    (_,_) => "UNKNOWN"
    }
}

fn method_carries_content(method_frame: &MethodFrame) -> bool {
    match (method_frame.class_id, method_frame.method_id) {
    <% class_id_and_method_id_carries_content.each do |m| -%>
    <%= m %>,
    <% end -%>
    (_,_) => false
    }
}

EOF

erb = ERB.new(method_frame_methods, 0 , "<>-")
File.write('src/method_frame_methods.rs', erb.result(SpecGenerator.new(SPEC.clone).get_binding))

erb = ERB.new(File.read('codegen.erb'), 0 , "<>-")
puts erb.result(SpecGenerator.new(SPEC.clone).get_binding)
