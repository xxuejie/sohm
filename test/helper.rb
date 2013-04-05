# encoding: UTF-8

$LOAD_PATH.unshift(File.expand_path(File.dirname(__FILE__) + "/../lib"))

begin
  require "ruby-debug"
rescue LoadError
end

require "rubygems"
require "cutest"

def silence_warnings
  original_verbose, $VERBOSE = $VERBOSE, nil
  yield
ensure
  $VERBOSE = original_verbose
end unless defined?(silence_warnings)

$VERBOSE = true

require "ohm"

Ohm.connect("redis://127.0.0.1:6379")

prepare do
  Ohm.redis.call("FLUSHALL")
end
