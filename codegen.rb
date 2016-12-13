gem 'json'
require 'json'
require 'erb'

# def pad_str(count, str)
#   str.lines.map{|line| "#{" "*count}#{line.lstrip}" }.join()
# end

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
      if klass["properties"] && klass["properties"].any?
        klass["properties_struct_name"] = "#{titleize(klass["name"])}Properties"
        klass["properties_fields"] = klass["properties"].map do |argument|
          [snake_name(argument["name"]), argument_type(argument)]
        end
      end
      klass["methods"].each do |method|
        method["method_name"] = camel_name titleize(method["name"])
        method["fields"]= method["arguments"].map do |argument|
          [snake_name(argument["name"]), argument_type(argument)]
        end
      end
    end
  end

  def argument_type(argument)
    argument["domain"] ? DOMAINS[argument["domain"]] : argument["type"]
  end

  def titleize(name)
    "#{name[0].upcase}#{name[1..-1]}"
  end

  def snake_name(name)
    name.tr("-","_").gsub(/^type$/, "_type")
  end

  # foo-bar => fooBar
  def camel_name(klass)
    klass.gsub(/(\-.)/){|c| c[1].upcase}
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
