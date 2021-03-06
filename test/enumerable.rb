require_relative "helper"

scope do
  class Contact < Sohm::Model
    include Sohm::AutoId
    include Sohm::IndexAll

    attribute :name
  end

  setup do
    john = Contact.create(name: "John Doe")
    jane = Contact.create(name: "Jane Doe")

    [john, jane]
  end

  test "Set#size doesn't do each" do
    set = Contact.all

    def set.each
      raise "Failed"
    end

    assert_equal 2, set.size
  end

  test "Set#each as an Enumerator" do |john, jane|
    enum = Contact.all.each

    enum.each do |c|
      assert c == john || c == jane
    end
  end

  test "select" do |john, jane|
    assert_equal 2, Contact.all.count
    assert_equal [john], Contact.all.select { |c| c.id == john.id }
  end
end

scope do
  class Comment < Sohm::Model
    include Sohm::AutoId
  end

  class Post < Sohm::Model
    include Sohm::AutoId

    list :comments, :Comment
  end

  setup do
    c1 = Comment.create
    c2 = Comment.create

    post = Post.create
    post.comments.push(c1)
    post.comments.push(c2)

    [post, c1, c2]
  end

  test "List#select" do |post, c1, c2|
    assert_equal [c1], post.comments.select { |comment| comment == c1 }
  end

  test "List#each as Enumerator" do |post, c1, c2|
    enum = post.comments.each

    enum.each do |comment|
      assert comment == c1 || comment == c2
    end
  end

  test "List#size doesn't do each" do |post, c1, c2|
    list = post.comments

    def list.each
      raise "Failed"
    end

    assert_equal 2, list.size
  end
end
