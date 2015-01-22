gem 'json'
require 'json'
require 'erb'

# def pad_str(count, str)
#   str.lines.map{|line| "#{" "*count}#{line.lstrip}" }.join()
# end

def read_type(type)
  case type
  when "octet"
    "try!(reader.read_byte())"
  when "long"
    "try!(reader.read_be_u32())"
  when "longlong"
    "try!(reader.read_be_u64())"
  when "short"
    "try!(reader.read_be_u16())"
  when "bit"
    raise "Cant read bit here..."
  when "shortstr"
    "{
          let size = try!(reader.read_byte()) as usize;
          String::from_utf8_lossy(&try!(reader.read_exact(size))[]).to_string()
     }"
  when "longstr"
    "{
          let size = try!(reader.read_be_u32()) as usize;
          String::from_utf8_lossy(&try!(reader.read_exact(size))[]).to_string()
      }"
  when "table"
    "try!(decode_table(reader))"
  when "timestamp"
    "try!(reader.read_be_u64())"
  else
    raise "Unknown type: #{type}"
  end
end

def write_type(name, type)
  case type
  when "octet"
    "writer.write_u8(#{name}).unwrap();"
  when "long"
    "writer.write_be_u32(#{name}).unwrap();"
  when "longlong"
    "writer.write_be_u64(#{name}).unwrap();"
  when "short"
    "writer.write_be_u16(#{name}).unwrap();"
  when "bit"
    raise "Cant write bit here..."
  when "shortstr"
    "writer.write_u8(#{name}.len() as u8).unwrap();
    writer.write(#{name}.as_bytes()).unwrap();"
  when "longstr"
    "writer.write_be_u32(#{name}.len() as u32).unwrap();
    writer.write(#{name}.as_bytes()).unwrap();"
  when "table"
    "encode_table(&mut writer, &#{name}).unwrap();"
  when "timestamp"
    "writer.write_be_u64(#{name}).unwrap();"
  else
    raise "Unknown type: #{type}"
  end
end

def generate_reader_body(arguments)
    body = []
    body << "let reader = &mut &method_frame.arguments[];"
    n_bits = 0
    arguments.each do |argument|
      type = argument["domain"] ? map_domain(argument["domain"]) : argument["type"]
      if type == "bit"
        if n_bits == 0
          body << "let byte = try!(reader.read_byte());"
          body << "let bits = Bitv::from_bytes(&[byte]);"
        end
        body << "let #{snake_name(argument["name"])} = bits.get(#{7-n_bits}).unwrap();"
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
    body << "let mut writer = vec!();"
    n_bits = 0
    arguments.each do |argument|
      type = argument["domain"] ? map_domain(argument["domain"]) : argument["type"]
      if type == "bit"
        if n_bits == 0
          body << "let mut bits = Bitv::from_elem(8, false);"
        end
        body << "bits.set(#{7-n_bits}, self.#{snake_name(argument["name"])});"
        n_bits += 1
        if n_bits == 8
          body << "writer.write(&bits.to_bytes()[]).unwrap();"
          n_bits = 0
        end
      else
        if n_bits > 0
          body << "writer.write(&bits.to_bytes()[]).unwrap();"
          n_bits = 0
        end
        body << write_type("self."+snake_name(argument["name"]), type)
      end
    end
    body << "writer.write(&bits.to_bytes()[]).unwrap();" if n_bits > 0 #if bits were the last element
    body << "writer"
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

  def matches
    @spec["classes"].flat_map do |klass|
      klass["methods"].map do |method|
        "(#{klass["id"]}, #{method["id"]}) => \"#{klass["name"]}.#{method["name"]}\""
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
      "\"#{value}\".to_string()"
    when Fixnum
      value
    when TrueClass, FalseClass
      value
    when Hash
      "table::new()"
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

erb = ERB.new(File.read('codegen.erb'), 0 , "<>-")
puts  erb.result(SpecGenerator.new(SPEC.clone).get_binding)
