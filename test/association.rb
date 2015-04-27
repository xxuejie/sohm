require_relative "helper"

class User < Sohm::Model
  include Sohm::AutoId

  collection :posts, :Post
end

class Post < Sohm::Model
  include Sohm::AutoId

  reference :user, :User
end

setup do
  u = User.create
  p = Post.create(:user => u)

  [u, p]
end

test "basic shake and bake" do |u, p|
  assert u.posts.include?(p)

  p = Post[p.id]
  assert_equal u, p.user
end
