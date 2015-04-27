require_relative 'helper'

test "model inherits Sohm.redis connection by default" do
  class C < Sohm::Model
  end

  assert_equal C.redis.url, Sohm.redis.url
end

test "model can define its own connection" do
  class B < Sohm::Model
    self.redis = Redic.new("redis://localhost:6379/1")
  end

  assert B.redis.url != Sohm.redis.url
end
