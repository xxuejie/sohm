Gem::Specification.new do |s|
  s.name = "sohm"
  s.version = "0.10.4"
  s.summary = %{Slim ohm for twemproxy-like system}
  s.description = %Q{Slim ohm is a forked ohm that works with twemproxy-like redis system, only a limited set of features in ohm is supported}
  s.authors = ["Xuejie Xiao"]
  s.email = ["xxuejie@gmail.com"]
  s.homepage = "https://github.com/xxuejie/sohm"
  s.license = "MIT"

  s.files = `git ls-files`.split("\n")

  s.add_dependency "redic", "~> 1.4.1"
  s.add_dependency "nido", "~> 0.0.1"
  s.add_dependency "msgpack", "~> 0.5.11"

  s.add_development_dependency "cutest", "~> 1.2.2"
end
