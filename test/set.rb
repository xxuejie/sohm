require_relative 'helper'

class Post < Sohm::Model
  include Sohm::AutoId
end

class User < Sohm::Model
  include Sohm::AutoId
  include Sohm::IndexAll

  attribute :name

  index :name

  set :posts, :Post
end

test '#exists? returns false if the given id is not included in the set' do
  assert !User.create.posts.exists?('nonexistent')
end

test '#exists? returns true if the given id is included in the set' do
  user = User.create
  post = Post.create
  user.posts.add(post)

  assert user.posts.exists?(post.id)
end
