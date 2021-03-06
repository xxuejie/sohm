# coding: utf-8
require_relative "helper"
require "ostruct"

class Post < Sohm::Model
  include Sohm::AutoId

  attribute :body
  attribute :published
  set :related, :Post
end

class User < Sohm::Model
  include Sohm::AutoId

  attribute :email
  set :posts, :Post
end

class Person < Sohm::Model
  include Sohm::AutoId
  include Sohm::IndexAll

  attribute :name
  counter :logins
  index :initial

  def initial
    name[0, 1].upcase if name
  end
end

class Event < Sohm::Model
  include Sohm::AutoId
  include Sohm::IndexAll

  attribute :name
  counter :votes
  set :attendees, :Person

  attribute :slug

  def save
    self.slug = name.to_s.downcase
    super
  end
end

module SomeNamespace
  class Foo < Sohm::Model
    include Sohm::AutoId

  attribute :name
  end

  class Bar < Sohm::Model
    include Sohm::AutoId

    reference :foo, 'SomeNamespace::Foo'
  end
end

class Meetup < Sohm::Model
  include Sohm::AutoId

  attribute :name
  attribute :location
end

test "booleans" do
  post = Post.new(body: true, published: false)

  post.save

  assert_equal true, post.body
  assert_equal false, post.published

  post = Post[post.id]

  assert_equal true, post.body
  assert_equal nil, post.published
end

test "empty model is ok" do
  class Foo < Sohm::Model
    include Sohm::AutoId
  end

  Foo.create
end

test "counters are cleaned up during deletion" do
  e = Event.create(:name => "Foo")
  e.incr :votes, 10

  assert_equal 10, e.votes

  e.delete
  assert_equal 0, Event.redis.call("EXISTS", e.key[:counters])
end

test "assign attributes from the hash" do
  event = Event.new(:name => "Ruby Tuesday")
  assert event.name == "Ruby Tuesday"
end

test "assign an ID and save the object" do
  event1 = Event.create(name: "Ruby Tuesday")
  event2 = Event.create(name: "Ruby Meetup")

  assert_equal "1", event1.id
  assert_equal "2", event2.id
end

test "updates attributes" do
  event = Meetup.create(:name => "Ruby Tuesday")
  event.update(:name => "Ruby Meetup")
  assert "Ruby Meetup" == event.name
end

test "save the attributes in UTF8" do
 event = Meetup.create(:name => "32° Kisei-sen")
 assert "32° Kisei-sen" == Meetup[event.id].name
end

test "delete the attribute if set to nil" do
  event = Meetup.create(:name => "Ruby Tuesday", :location => "Los Angeles")
  assert "Los Angeles" == Meetup[event.id].location
  assert event.update(:location => nil)
  assert_equal nil, Meetup[event.id].location
end

test "not raise if an attribute is redefined" do
  class RedefinedModel < Sohm::Model
    include Sohm::AutoId

    attribute :name

    silence_warnings do
      attribute :name
    end
  end
end

test "not raise if a counter is redefined" do
  class RedefinedModel < Sohm::Model
    include Sohm::AutoId

    counter :age

    silence_warnings do
      counter :age
    end
  end
end

test "not raise if a set is redefined" do
  class RedefinedModel < Sohm::Model
    include Sohm::AutoId

    set :friends, lambda { }

    silence_warnings do
      set :friends, lambda { }
    end
  end
end

test "not raise if a collection is redefined" do
  class RedefinedModel < Sohm::Model
    include Sohm::AutoId

    set :toys, lambda { }

    silence_warnings do
      set :toys, lambda { }
    end
  end
end

test "not raise if a index is redefined" do
  class RedefinedModel < Sohm::Model
    include Sohm::AutoId

    attribute :color
    index :color
    index :color
  end
end

test "allow arbitrary IDs" do
  Event.create(:id => "abc123", :name => "Concert")

  assert Event.all.size == 1
  assert Event["abc123"].name == "Concert"
end

setup do
  Sohm.redis.call("SADD", "Event:all", 1)
  Sohm.redis.call("HSET", "Event:1", "name", "Concert")
end

test "return an instance of Event" do
  assert Event[1].kind_of?(Event)
  assert 1 == Event[1].id
  assert "Concert" == Event[1].name
end

setup do
  Sohm.redis.call("SADD", "User:all", 1)
  Sohm.redis.call("HSET", "User:1", "email", "albert@example.com")
end

test "return an instance of User" do
  assert User[1].kind_of?(User)
  assert 1 == User[1].id
  assert "albert@example.com" == User[1].email
end

test "allow to map key to models" do
  assert [User[1]] == [1].map(&User)
end

setup do
  Sohm.redis.call("INCR", "User:_id")
  Sohm.redis.call("HSET", "User:1", "_ndata", "\x81\xa5email\xb2albert@example.com")

  @user = User[1]
end

test "change its attributes" do
  @user.email = "maria@example.com"
  assert "maria@example.com" == @user.email
end

test "save the new values" do
  @user.email = "maria@example.com"
  @user.save

  @user.email = "maria@example.com"
  @user.save

  assert "maria@example.com" == User[1].email
end

test "assign a new id to the event" do
  event1 = Event.new
  event1.save

  event2 = Event.new
  event2.save

  assert !event1.new?
  assert !event2.new?

  assert_equal "1", event1.id
  assert_equal "2", event2.id
end

# Saving a model
test "create the model if it is new" do
  event = Event.new(:name => "Foo").save
  assert "Foo" == Event[event.id].name
end

test "save it only if it was previously created" do
  event = Event.new
  event.name = "Lorem ipsum"
  event.save

  event.name = "Lorem"
  event.save

  assert "Lorem" == Event[event.id].name
end

test "allow to hook into save" do
  event = Event.create(:name => "Foo")

  assert "foo" == event.slug
end

test "save counters" do
  event = Event.create(:name => "Foo")

  event.incr(:votes)
  event.save

  assert_equal 1, Event[event.id].votes
end

# Delete
test "delete an existing model" do
  class ModelToBeDeleted < Sohm::Model
    include Sohm::AutoId
    include Sohm::IndexAll

    attribute :name
    set :foos, :Post
    set :bars, :Post
  end

  @model = ModelToBeDeleted.create(:name => "Lorem")

  @model.foos.add(Post.create)
  @model.bars.add(Post.create)

  id = @model.id

  @model.delete

  assert Sohm.redis.call("GET", ModelToBeDeleted.key[id]).nil?
  assert Sohm.redis.call("GET", ModelToBeDeleted.key[id][:name]).nil?
  assert Array.new == Sohm.redis.call("SMEMBERS", ModelToBeDeleted.key[id][:foos])
  assert Array.new == Sohm.redis.call("LRANGE", ModelToBeDeleted.key[id][:bars], 0, -1)

  assert ModelToBeDeleted.all.empty?
end

setup do
end

test "no leftover keys" do
  class ::Foo < Sohm::Model
    include Sohm::AutoId

    attribute :name
    index :name
    track :notes
  end

  assert_equal [], Sohm.redis.call("KEYS", "*")

  Foo.create(:name => "Bar")
  expected = %w[Foo:1:_indices Foo:1 Foo:_id Foo:_indices:name:Bar]

  assert_equal expected.sort, Sohm.redis.call("KEYS", "*").sort

  Foo[1].delete
  assert ["Foo:_id"] == Sohm.redis.call("KEYS", "*")

  Foo.create(:name => "Baz")

  Sohm.redis.call("SET", Foo[2].key[:notes], "something")

  expected = %w[Foo:2:_indices Foo:2 Foo:_id
    Foo:_indices:name:Baz Foo:2:notes]

  assert_equal expected.sort, Sohm.redis.call("KEYS", "*").sort

  Foo[2].delete
  assert ["Foo:_id"] == Sohm.redis.call("KEYS", "*")
end

# Listing
test "find all" do
  event1 = Event.new
  event1.name = "Ruby Meetup"
  event1.save

  event2 = Event.new
  event2.name = "Ruby Tuesday"
  event2.save

  all = Event.all
  assert all.detect {|e| e.name == "Ruby Meetup" }
  assert all.detect {|e| e.name == "Ruby Tuesday" }
end

# Fetching
test "fetch ids" do
  event1 = Event.create(:name => "A")
  event2 = Event.create(:name => "B")

  assert_equal [event1, event2], Event.fetch([event1.id, event2.id])
end

# Loading attributes
setup do
  event = Event.new
  event.name = "Ruby Tuesday"
  event.save.id
end

test "load attributes as native type" do
  event = Event.create(:name => 1)

  assert 1 == Event[event.id].name
end

# Enumerable indices
class Entry < Sohm::Model
  include Sohm::AutoId

  attribute :tags
  index :tag

  def tag
    tags.split(/\s+/)
  end
end

setup do
  Entry.create(:tags => "foo bar baz")
end

test "finding by one entry in the enumerable" do |entry|
  assert Entry.find(:tag => "foo").include?(entry)
  assert Entry.find(:tag => "bar").include?(entry)
  assert Entry.find(:tag => "baz").include?(entry)
end

# Attributes of type Set
setup do
  @person1 = Person.create(:name => "Albert")
  @person2 = Person.create(:name => "Bertrand")
  @person3 = Person.create(:name => "Charles")

  @event = Event.new
  @event.name = "Ruby Tuesday"
end

test "delete elements" do
  @event.save
  @event.attendees.add(@person1)
  @event.attendees.add(@person2)

  assert_equal 2, @event.attendees.size

  @event.attendees.delete(@person2)
  assert_equal 1, @event.attendees.size
end

test "not be available if the model is new" do
  assert_raise Sohm::MissingID do
    @event.attendees
  end
end

test "remove an element if sent delete" do
  @event.save
  @event.attendees.add(@person1)
  @event.attendees.add(@person2)
  @event.attendees.add(@person3)

  assert_equal ["1", "2", "3"], Event.redis.call("SORT", @event.attendees.key)

  Event.redis.call("SREM", @event.attendees.key, @person2.id)
  assert_equal ["1", "3"], Event.redis.call("SORT", Event[@event.id].attendees.key)
end

test "return true if the set includes some member" do
  @event.save
  @event.attendees.add(@person1)
  @event.attendees.add(@person2)
  assert @event.attendees.include?(@person2)

  @event.attendees.include?(@person3)
  assert !@event.attendees.include?(@person3)
end

test "return instances of the passed model" do
  @event.save
  @event.attendees.add(@person1)

  assert [@person1] == @event.attendees.to_a
  assert @person1 == @event.attendees[@person1.id]
end

test "return the size of the set" do
  @event.save
  @event.attendees.add(@person1)
  @event.attendees.add(@person2)
  @event.attendees.add(@person3)
  assert 3 == @event.attendees.size
end

test "empty the set" do
  @event.save
  @event.attendees.add(@person1)

  Event.redis.call("DEL", @event.attendees.key)

  assert @event.attendees.empty?
end

# Collections initialized with a Model parameter
setup do
  @user = User.create(:email => "albert@example.com")
  @user.posts.add(Post.create(:body => "D"))
  @user.posts.add(Post.create(:body => "C"))
  @user.posts.add(Post.create(:body => "B"))
  @user.posts.add(Post.create(:body => "A"))
end

test "return instances of the passed model" do
  assert Post == @user.posts.first.class
end

test "remove an object from the set" do
  post = @user.posts.first
  assert @user.posts.include?(post)

  User.redis.call("SREM", @user.posts.key, post.id)
  assert !@user.posts.include?(post)
end

test "remove an object id from the set" do
  post = @user.posts.first
  assert @user.posts.include?(post)

  User.redis.call("SREM", @user.posts.key, post.id)
  assert !@user.posts.include?(post)
end

# Counters
setup do
  @event = Event.create(:name => "Ruby Tuesday")
end

test "be zero if not initialized" do
  assert 0 == @event.votes
end

test "be able to increment a counter" do
  @event.incr(:votes)
  assert 1 == @event.votes

  @event.incr(:votes, 2)
  assert 3 == @event.votes
end

test "be able to decrement a counter" do
  @event.decr(:votes)
  assert @event.votes == -1

  @event.decr(:votes, 2)
  assert @event.votes == -3
end

# Comparison
setup do
  @user = User.create(:email => "foo")
end

test "be comparable to other instances" do
  assert @user == User[@user.id]

  assert @user != User.create
  assert User.new != User.new
end

test "not be comparable to instances of other models" do
  assert @user != Event.create(:name => "Ruby Tuesday")
end

test "be comparable to non-models" do
  assert @user != 1
  assert @user != true

  # Not equal although the other object responds to #key.
  assert @user != OpenStruct.new(:key => @user.send(:key))
end

# Debugging
class ::Bar < Sohm::Model
  include Sohm::AutoId

  attribute :name
  counter :visits
  set :friends, self
  set :comments, self

  def foo
    bar.foo
  end

  def baz
    bar.new.foo
  end

  def bar
    SomeMissingConstant
  end
end

# Models connected to different databases
class ::Car < Sohm::Model
  include Sohm::AutoId
  include Sohm::IndexAll

  attribute :name

  self.redis = Redic.new
end

class ::Make < Sohm::Model
  include Sohm::AutoId
  include Sohm::IndexAll

  attribute :name
end

setup do
  Car.redis.call("SELECT", 15)
end

test "save to the selected database" do
  car = Car.create(:name => "Twingo")
  make = Make.create(:name => "Renault")

  redis = Redic.new

  assert ["1"] == redis.call("SMEMBERS", "Make:_indices:all:all")
  assert [] == redis.call("SMEMBERS", "Car:_indices:all:all")

  assert ["1"] == Car.redis.call("SMEMBERS", "Car:_indices:all:all")
  assert [] == Car.redis.call("SMEMBERS", "Make:_indices:all:all")

  assert car == Car[1]
  assert make == Make[1]

  Make.redis.call("FLUSHDB")

  assert car == Car[1]
  assert Make[1].nil?
end

test "allow changing the database" do
  Car.create(:name => "Twingo")
  assert_equal ["1"], Car.redis.call("SMEMBERS", Car.all.key)

  Car.redis = Redic.new("redis://127.0.0.1:6379")
  assert_equal [], Car.redis.call("SMEMBERS", Car.all.key)

  Car.redis.call("SELECT", 15)
  assert_equal ["1"], Car.redis.call("SMEMBERS", Car.all.key)
end

# Persistence
test "persist attributes to a hash" do
  event = Event.create(:name => "Redis Meetup")
  event.incr(:votes)

  assert "hash" == Sohm.redis.call("TYPE", "Event:1")

  expected= %w[Event:1 Event:1:_indices Event:_indices:all:all Event:1:_counters Event:_id]
  assert_equal expected.sort, Sohm.redis.call("KEYS", "Event:*").sort

  assert "Redis Meetup" == Event[1].name
  assert 1 == Event[1].votes
end

# namespaced models
test "be persisted" do
  SomeNamespace::Foo.create(:name => "foo")

  SomeNamespace::Bar.create(:foo  => SomeNamespace::Foo[1])

  assert "hash" == Sohm.redis.call("TYPE", "SomeNamespace::Foo:1")

  assert "foo" == SomeNamespace::Foo[1].name
  assert "foo" == SomeNamespace::Bar[1].foo.name
end if RUBY_VERSION >= "2.0.0"

test "typecast attributes" do
  class Option < Sohm::Model
    include Sohm::AutoId

    attribute :votes, lambda { |x| x.to_i }
  end

  option = Option.create :votes => 20
  option.update(:votes => option.votes + 1)

  assert_equal 21, option.votes
end

test "poster-example for overriding writers" do
  silence_warnings do
    class Advertiser < Sohm::Model
      include Sohm::AutoId

      attribute :email

      def email=(e)
        attributes[:email] = e.to_s.downcase.strip
      end
    end
  end

  a = Advertiser.new(:email => " FOO@BAR.COM ")
  assert_equal "foo@bar.com", a.email
end
