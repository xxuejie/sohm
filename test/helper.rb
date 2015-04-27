begin
  require "ruby-debug"
rescue LoadError
end

require "cutest"

def silence_warnings
  original_verbose, $VERBOSE = $VERBOSE, nil
  yield
ensure
  $VERBOSE = original_verbose
end unless defined?(silence_warnings)

$VERBOSE = true

require_relative "../lib/sohm"
require_relative "../lib/sohm/auto_id"
require_relative "../lib/sohm/index_all"

Sohm.redis = Redic.new("redis://127.0.0.1:6379")

prepare do
  Sohm.redis.call("FLUSHALL")
end
