# encoding: UTF-8

require 'digest/sha1'
require "msgpack"
require "nido"
require "redic"
require "securerandom"
require "set"
require_relative "sohm/command"

module Sohm
  LUA_SAVE = File.read(File.expand_path("../sohm/lua/save.lua",   __FILE__))
  LUA_SAVE_DIGEST = Digest::SHA1.hexdigest LUA_SAVE

  # All of the known errors in Ohm can be traced back to one of these
  # exceptions.
  #
  # MissingID:
  #
  #   Comment.new.id # => Error
  #   Comment.new.key # => Error
  #
  #   Solution: you need to save your model first.
  #
  # IndexNotFound:
  #
  #   Comment.find(:foo => "Bar") # => Error
  #
  #   Solution: add an index with `Comment.index :foo`.
  #
  # UniqueIndexViolation:
  #
  #   Raised when trying to save an object with a `unique` index for
  #   which the value already exists.
  #
  #   Solution: rescue `Sohm::UniqueIndexViolation` during save, but
  #   also, do some validations even before attempting to save.
  #
  class Error < StandardError; end
  class MissingID < Error; end
  class IndexNotFound < Error; end
  class CasViolation < Error; end

  # Instead of monkey patching Kernel or trying to be clever, it's
  # best to confine all the helper methods in a Utils module.
  module Utils

    # Used by: `attribute`, `counter`, `set`, `reference`,
    # `collection`.
    #
    # Employed as a solution to avoid `NameError` problems when trying
    # to load models referring to other models not yet loaded.
    #
    # Example:
    #
    #   class Comment < Sohm::Model
    #     reference :user, User # NameError undefined constant User.
    #   end
    #
    #   # Instead of relying on some clever `const_missing` hack, we can
    #   # simply use a symbol or a string.
    #
    #   class Comment < Sohm::Model
    #     reference :user, :User
    #     reference :post, "Post"
    #   end
    #
    def self.const(context, name)
      case name
      when Symbol, String
        context.const_get(name)
      else name
      end
    end

    def self.dict(arr)
      Hash[*arr]
    end
  end

  # Use this if you want to do quick ad hoc redis commands against the
  # defined Ohm connection.
  #
  # Examples:
  #
  #   Ohm.redis.call("SET", "foo", "bar")
  #   Ohm.redis.call("FLUSH")
  #
  @redis = Redic.new
  def self.redis
    @redis
  end

  def self.redis=(redis)
    @redis = redis
  end

  # If you are using a Redis pool to override @redis above, chances are
  # you won't need a mutex(since your opertions will run on different
  # redis instances), so you can use this to override the default mutex
  # for better performance
  @mutex = Mutex.new
  def self.mutex=(mutex)
    @mutex = mutex
  end

  def self.mutex
    @mutex
  end

  # By default, EVALSHA is used
  def self.enable_evalsha
    defined?(@enable_evalsha) ? @enable_evalsha : true
  end

  def self.enable_evalsha=(enabled)
    @enable_evalsha = enabled
  end

  module Collection
    include Enumerable

    def each
      if block_given?
        ids.each_slice(1000) do |slice|
          fetch(slice).each { |e| yield(e) }
        end
      else
        to_enum
      end
    end

    # Fetch the data from Redis in one go.
    def to_a
      fetch(ids)
    end

    def empty?
      size == 0
    end

    # Wraps the whole pipelining functionality.
    def fetch(ids)
      data = nil

      model.synchronize do
        ids.each do |id|
          redis.queue("HGETALL", namespace[id])
        end

        data = redis.commit
      end

      return [] if data.nil?

      [].tap do |result|
        data.each_with_index do |atts, idx|
          unless atts.empty?
            result << model.new(Utils.dict(atts).update(:id => ids[idx]))
          end
        end
      end
    end
  end

  class List
    include Collection

    attr :key
    attr :namespace
    attr :model

    def initialize(key, namespace, model)
      @key = key
      @namespace = namespace
      @model = model
    end

    # Returns the total size of the list using LLEN.
    def size
      redis.call("LLEN", key)
    end

    # Returns the first element of the list using LINDEX.
    def first
      model[redis.call("LINDEX", key, 0)]
    end

    # Returns the last element of the list using LINDEX.
    def last
      model[redis.call("LINDEX", key, -1)]
    end

    # Returns an array of elements from the list using LRANGE.
    # #range receives 2 integers, start and stop
    #
    # Example:
    #
    #   class Comment < Sohm::Model
    #   end
    #
    #   class Post < Sohm::Model
    #     list :comments, :Comment
    #   end
    #
    #   c1 = Comment.create
    #   c2 = Comment.create
    #   c3 = Comment.create
    #
    #   post = Post.create
    #
    #   post.comments.push(c1)
    #   post.comments.push(c2)
    #   post.comments.push(c3)
    #
    #   [c1, c2] == post.comments.range(0, 1)
    #   # => true
    def range(start, stop)
      fetch(redis.call("LRANGE", key, start, stop))
    end

    # Checks if the model is part of this List.
    #
    # An important thing to note is that this method loads all of the
    # elements of the List since there is no command in Redis that
    # allows you to actually check the list contents efficiently.
    #
    # You may want to avoid doing this if your list has say, 10K entries.
    def include?(model)
      ids.include?(model.id)
    end

    # Pushes the model to the _end_ of the list using RPUSH.
    def push(model)
      redis.call("RPUSH", key, model.id)
    end

    # Pushes the model to the _beginning_ of the list using LPUSH.
    def unshift(model)
      redis.call("LPUSH", key, model.id)
    end

    # Delete a model from the list.
    #
    # Note: If your list contains the model multiple times, this method
    # will delete all instances of that model in one go.
    #
    # Example:
    #
    #   class Comment < Sohm::Model
    #   end
    #
    #   class Post < Sohm::Model
    #     list :comments, :Comment
    #   end
    #
    #   p = Post.create
    #   c = Comment.create
    #
    #   p.comments.push(c)
    #   p.comments.push(c)
    #
    #   p.comments.delete(c)
    #
    #   p.comments.size == 0
    #   # => true
    #
    def delete(model)
      # LREM key 0 <id> means remove all elements matching <id>
      # @see http://redis.io/commands/lrem
      redis.call("LREM", key, 0, model.id)
    end

    # Returns an array with all the ID's of the list.
    #
    #   class Comment < Sohm::Model
    #   end
    #
    #   class Post < Sohm::Model
    #     list :comments, :Comment
    #   end
    #
    #   post = Post.create
    #   post.comments.push(Comment.create)
    #   post.comments.push(Comment.create)
    #   post.comments.push(Comment.create)
    #
    #   post.comments.map(&:id)
    #   # => ["1", "2", "3"]
    #
    #   post.comments.ids
    #   # => ["1", "2", "3"]
    #
    def ids
      redis.call("LRANGE", key, 0, -1)
    end

  private

    def redis
      model.redis
    end
  end

  # Defines most of the methods used by `Set` and `MultiSet`.
  class BasicSet
    include Collection

    # Check if a model is included in this set.
    #
    # Example:
    #
    #   u = User.create
    #
    #   User.all.include?(u)
    #   # => true
    #
    # Note: Ohm simply checks that the model's ID is included in the
    # set. It doesn't do any form of type checking.
    #
    def include?(model)
      exists?(model.id)
    end

    # Returns the total size of the set using SCARD.
    def size
      execute { |key| redis.call("SCARD", key) }
    end

    # SMEMBERS then choosing the first will take too much memory in case data
    # grow big enough, which will be slow in this case.
    # Providing +sample+ only gives a hint that we won't preserve any order
    # for this, there will be 2 cases:
    #
    # 1. Anyone in the set will do, this is the original use case of +sample*
    # 2. For some reasons(maybe due to filters), we only have 1 element left
    # in this set, using +sample+ will do the trick
    #
    # For all the other cases, we won't be able to fetch a single element
    # without fetching all elements first(in other words, doing this
    # efficiently)
    def sample
      model[execute { |key| redis.call("SRANDMEMBER", key) }]
    end

    # Returns an array with all the ID's of the set.
    #
    #   class Post < Sohm::Model
    #   end
    #
    #   class User < Sohm::Model
    #     attribute :name
    #     index :name
    #
    #     set :posts, :Post
    #   end
    #
    #   User.create(name: "John")
    #   User.create(name: "Jane")
    #
    #   User.all.ids
    #   # => ["1", "2"]
    #
    #   User.find(name: "John").union(name: "Jane").ids
    #   # => ["1", "2"]
    #
    def ids
      execute { |key| redis.call("SMEMBERS", key) }
    end

    # Retrieve a specific element using an ID from this set.
    #
    # Example:
    #
    #   # Let's say we got the ID 1 from a request parameter.
    #   id = 1
    #
    #   # Retrieve the post if it's included in the user's posts.
    #   post = user.posts[id]
    #
    def [](id)
      model[id] if exists?(id)
    end

    # Returns +true+ if +id+ is included in the set. Otherwise, returns +false+.
    #
    # Example:
    #
    #   class Post < Sohm::Model
    #   end
    #
    #   class User < Sohm::Model
    #     set :posts, :Post
    #   end
    #
    #   user = User.create
    #   post = Post.create
    #   user.posts.add(post)
    #
    #   user.posts.exists?('nonexistent') # => false
    #   user.posts.exists?(post.id)       # => true
    #
    def exists?(id)
      execute { |key| redis.call("SISMEMBER", key, id) == 1 }
    end
  end

  class Set < BasicSet
    attr :key
    attr :namespace
    attr :model

    def initialize(key, namespace, model)
      @key = key
      @namespace = namespace
      @model = model
    end

    # Chain new fiters on an existing set.
    #
    # Example:
    #
    #   set = User.find(:name => "John")
    #   set.find(:age => 30)
    #
    def find(dict)
      MultiSet.new(
        namespace, model, Command[:sinterstore, key, *model.filters(dict)]
      )
    end

    # Reduce the set using any number of filters.
    #
    # Example:
    #
    #   set = User.find(:name => "John")
    #   set.except(:country => "US")
    #
    #   # You can also do it in one line.
    #   User.find(:name => "John").except(:country => "US")
    #
    def except(dict)
      MultiSet.new(namespace, model, key).except(dict)
    end

    # Perform an intersection between the existent set and
    # the new set created by the union of the passed filters.
    #
    # Example:
    #
    #   set = User.find(:status => "active")
    #   set.combine(:name => ["John", "Jane"])
    #   
    #   # The result will include all users with active status
    #   # and with names "John" or "Jane".
    def combine(dict)
      MultiSet.new(namespace, model, key).combine(dict)
    end

    # Do a union to the existing set using any number of filters.
    #
    # Example:
    #
    #   set = User.find(:name => "John")
    #   set.union(:name => "Jane")
    #
    #   # You can also do it in one line.
    #   User.find(:name => "John").union(:name => "Jane")
    #
    def union(dict)
      MultiSet.new(namespace, model, key).union(dict)
    end

  private
    def execute
      yield key
    end

    def redis
      model.redis
    end
  end

  class MutableSet < Set
    # Add a model directly to the set.
    #
    # Example:
    #
    #   user = User.create
    #   post = Post.create
    #
    #   user.posts.add(post)
    #
    def add(model)
      redis.call("SADD", key, model.id)
    end

    alias_method :<<, :add

    # Remove a model directly from the set.
    #
    # Example:
    #
    #   user = User.create
    #   post = Post.create
    #
    #   user.posts.delete(post)
    #
    def delete(model)
      redis.call("SREM", key, model.id)
    end
  end

  # Anytime you filter a set with more than one requirement, you
  # internally use a `MultiSet`. `MultiSet` is a bit slower than just
  # a `Set` because it has to `SINTERSTORE` all the keys prior to
  # retrieving the members, size, etc.
  #
  # Example:
  #
  #   User.all.kind_of?(Sohm::Set)
  #   # => true
  #
  #   User.find(:name => "John").kind_of?(Sohm::Set)
  #   # => true
  #
  #   User.find(:name => "John", :age => 30).kind_of?(Sohm::MultiSet)
  #   # => true
  #
  class MultiSet < BasicSet
    attr :namespace
    attr :model
    attr :command

    def initialize(namespace, model, command)
      @namespace = namespace
      @model = model
      @command = command
    end

    # Chain new fiters on an existing set.
    #
    # Example:
    #
    #   set = User.find(:name => "John", :age => 30)
    #   set.find(:status => 'pending')
    #
    def find(dict)
      MultiSet.new(
        namespace, model, Command[:sinterstore, command, intersected(dict)]
      )
    end

    # Reduce the set using any number of filters.
    #
    # Example:
    #
    #   set = User.find(:name => "John")
    #   set.except(:country => "US")
    #
    #   # You can also do it in one line.
    #   User.find(:name => "John").except(:country => "US")
    #
    def except(dict)
      MultiSet.new(
        namespace, model, Command[:sdiffstore, command, unioned(dict)]
      )
    end

    # Perform an intersection between the existent set and
    # the new set created by the union of the passed filters.
    #
    # Example:
    #
    #   set = User.find(:status => "active")
    #   set.combine(:name => ["John", "Jane"])
    #   
    #   # The result will include all users with active status
    #   # and with names "John" or "Jane".
    def combine(dict)
      MultiSet.new(
        namespace, model, Command[:sinterstore, command, unioned(dict)]
      )
    end

    # Do a union to the existing set using any number of filters.
    #
    # Example:
    #
    #   set = User.find(:name => "John")
    #   set.union(:name => "Jane")
    #
    #   # You can also do it in one line.
    #   User.find(:name => "John").union(:name => "Jane")
    #
    def union(dict)
      MultiSet.new(
        namespace, model, Command[:sunionstore, command, intersected(dict)]
      )
    end

  private
    def redis
      model.redis
    end

    def intersected(dict)
      Command[:sinterstore, *model.filters(dict)]
    end

    def unioned(dict)
      Command[:sunionstore, *model.filters(dict)]
    end

    def execute
      # namespace[:tmp] is where all the temp keys should be stored in.
      # redis will be where all the commands are executed against.
      response = command.call(namespace[:tmp], redis)

      begin

        # At this point, we have the final aggregated set, which we yield
        # to the caller. the caller can do all the normal set operations,
        # i.e. SCARD, SMEMBERS, etc.
        yield response

      ensure

        # We have to make sure we clean up the temporary keys to avoid
        # memory leaks and the unintended explosion of memory usage.
        command.clean
      end
    end
  end

  # The base class for all your models. In order to better understand
  # it, here is a semi-realtime explanation of the details involved
  # when creating a User instance.
  #
  # Example:
  #
  #   class User < Sohm::Model
  #     attribute :name
  #     index :name
  #
  #     attribute :email
  #     unique :email
  #
  #     counter :points
  #
  #     set :posts, :Post
  #   end
  #
  #   u = User.create(:name => "John", :email => "foo@bar.com")
  #   u.incr :points
  #   u.posts.add(Post.create)
  #
  # When you execute `User.create(...)`, you run the following Redis
  # commands:
  #
  #   # Generate an ID
  #   INCR User:id
  #
  #   # Add the newly generated ID, (let's assume the ID is 1).
  #   SADD User:all 1
  #
  #   # Store the unique index
  #   HSET User:uniques:email foo@bar.com 1
  #
  #   # Store the name index
  #   SADD User:indices:name:John 1
  #
  #   # Store the HASH
  #   HMSET User:1 name John email foo@bar.com
  #
  # Next we increment points:
  #
  #   HINCR User:1:_counters points 1
  #
  # And then we add a Post to the `posts` set.
  # (For brevity, let's assume the Post created has an ID of 1).
  #
  #   SADD User:1:posts 1
  #
  class Model
    def self.redis=(redis)
      @redis = redis
    end

    def self.redis
      defined?(@redis) ? @redis : Sohm.redis
    end

    def self.mutex
      Sohm.mutex
    end

    def self.synchronize(&block)
      mutex.synchronize(&block)
    end

    # Returns the namespace for all the keys generated using this model.
    #
    # Example:
    #
    #   class User < Sohm::Model
    #   end
    #
    #   User.key == "User"
    #   User.key.kind_of?(String)
    #   # => true
    #
    #   User.key.kind_of?(Nido)
    #   # => true
    #
    # To find out more about Nido, see:
    #   http://github.com/soveran/nido
    #
    def self.key
      Nido.new(self.name)
    end

    # Retrieve a record by ID.
    #
    # Example:
    #
    #   u = User.create
    #   u == User[u.id]
    #   # =>  true
    #
    def self.[](id)
      new(:id => id).load! if id && exists?(id)
    end

    # Retrieve a set of models given an array of IDs.
    #
    # Example:
    #
    #   ids = [1, 2, 3]
    #   ids.map(&User)
    #
    # Note: The use of this should be a last resort for your actual
    # application runtime, or for simply debugging in your console. If
    # you care about performance, you should pipeline your reads. For
    # more information checkout the implementation of Sohm::List#fetch.
    #
    def self.to_proc
      lambda { |id| self[id] }
    end

    # Check if the ID exists within <Model>:all.
    def self.exists?(id)
      redis.call("EXISTS", key[id]) == 1
    end

    # Find values in indexed fields.
    #
    # Example:
    #
    #   class User < Sohm::Model
    #     attribute :email
    #
    #     attribute :name
    #     index :name
    #
    #     attribute :status
    #     index :status
    #
    #     index :provider
    #     index :tag
    #
    #     def provider
    #       email[/@(.*?).com/, 1]
    #     end
    #
    #     def tag
    #       ["ruby", "python"]
    #     end
    #   end
    #
    #   u = User.create(name: "John", status: "pending", email: "foo@me.com")
    #   User.find(provider: "me", name: "John", status: "pending").include?(u)
    #   # => true
    #
    #   User.find(:tag => "ruby").include?(u)
    #   # => true
    #
    #   User.find(:tag => "python").include?(u)
    #   # => true
    #
    #   User.find(:tag => ["ruby", "python"]).include?(u)
    #   # => true
    #
    def self.find(dict)
      keys = filters(dict)

      raise "Not supported for now!" unless keys.size == 1
      Sohm::Set.new(keys.first, key, self)
    end

    # Retrieve a set of models given an array of IDs.
    #
    # Example:
    #
    #   User.fetch([1, 2, 3])
    #
    def self.fetch(ids)
      all.fetch(ids)
    end

    # Index any method on your model. Once you index a method, you can
    # use it in `find` statements.
    def self.index(attribute)
      indices << attribute unless indices.include?(attribute)
    end

    # Declare an Sohm::Set with the given name.
    #
    # Example:
    #
    #   class User < Sohm::Model
    #     set :posts, :Post
    #   end
    #
    #   u = User.create
    #   u.posts.empty?
    #   # => true
    #
    # Note: You can't use the set until you save the model. If you try
    # to do it, you'll receive an Sohm::MissingID error.
    #
    def self.set(name, model)
      track(name)

      define_method name do
        model = Utils.const(self.class, model)

        Sohm::MutableSet.new(key[name], model.key, model)
      end
    end

    # Declare an Sohm::List with the given name.
    #
    # Example:
    #
    #   class Comment < Sohm::Model
    #   end
    #
    #   class Post < Sohm::Model
    #     list :comments, :Comment
    #   end
    #
    #   p = Post.create
    #   p.comments.push(Comment.create)
    #   p.comments.unshift(Comment.create)
    #   p.comments.size == 2
    #   # => true
    #
    # Note: You can't use the list until you save the model. If you try
    # to do it, you'll receive an Sohm::MissingID error.
    #
    def self.list(name, model)
      track(name)

      define_method name do
        model = Utils.const(self.class, model)

        Sohm::List.new(key[name], model.key, model)
      end
    end

    # A macro for defining a method which basically does a find.
    #
    # Example:
    #   class Post < Sohm::Model
    #     reference :user, :User
    #   end
    #
    #   class User < Sohm::Model
    #     collection :posts, :Post
    #   end
    #
    #   # is the same as
    #
    #   class User < Sohm::Model
    #     def posts
    #       Post.find(:user_id => self.id)
    #     end
    #   end
    #
    def self.collection(name, model, reference = to_reference)
      define_method name do
        model = Utils.const(self.class, model)
        model.find(:"#{reference}_id" => id)
      end
    end

    # A macro for defining an attribute, an index, and an accessor
    # for a given model.
    #
    # Example:
    #
    #   class Post < Sohm::Model
    #     reference :user, :User
    #   end
    #
    #   # It's the same as:
    #
    #   class Post < Sohm::Model
    #     attribute :user_id
    #     index :user_id
    #
    #     def user
    #       User[user_id]
    #     end
    #
    #     def user=(user)
    #       self.user_id = user.id
    #     end
    #
    #     def user_id=(user_id)
    #       self.user_id = user_id
    #     end
    #   end
    #
    def self.reference(name, model)
      reader = :"#{name}_id"
      writer = :"#{name}_id="

      attributes << reader unless attributes.include?(reader)

      index reader

      define_method(reader) do
        @attributes[reader]
      end

      define_method(writer) do |value|
        @attributes[reader] = value
      end

      define_method(:"#{name}=") do |value|
        send(writer, value ? value.id : nil)
      end

      define_method(name) do
        model = Utils.const(self.class, model)
        model[send(reader)]
      end
    end

    # The bread and butter macro of all models. Basically declares
    # persisted attributes. All attributes are stored on the Redis
    # hash.
    #
    #   class User < Sohm::Model
    #     attribute :name
    #   end
    #
    #   user = User.new(name: "John")
    #   user.name
    #   # => "John"
    #
    #   user.name = "Jane"
    #   user.name
    #   # => "Jane"
    #
    # A +lambda+ can be passed as a second parameter to add
    # typecasting support to the attribute.
    #
    #   class User < Sohm::Model
    #     attribute :age, ->(x) { x.to_i }
    #   end
    #
    #   user = User.new(age: 100)
    #
    #   user.age
    #   # => 100
    #
    #   user.age.kind_of?(Integer)
    #   # => true
    #
    # Check http://rubydoc.info/github/cyx/ohm-contrib#Ohm__DataTypes
    # to see more examples about the typecasting feature.
    #
    def self.attribute(name, cast = nil)
      if serial_attributes.include?(name)
        raise ArgumentError,
              "#{name} is already used as a serial attribute."
      end
      attributes << name unless attributes.include?(name)

      if cast
        define_method(name) do
          cast[@attributes[name]]
        end
      else
        define_method(name) do
          @attributes[name]
        end
      end

      define_method(:"#{name}=") do |value|
        @attributes[name] = value
      end
    end

    # Attributes that require CAS property
    def self.serial_attribute(name, cast = nil)
      if attributes.include?(name)
        raise ArgumentError,
              "#{name} is already used as a normal attribute."
      end
      serial_attributes << name unless serial_attributes.include?(name)

      if cast
        define_method(name) do
          # NOTE: This is a temporary solution, since we might use
          # composite objects (such as arrays), which won't always
          # do a reset
          @serial_attributes_changed = true
          cast[@serial_attributes[name]]
        end
      else
        define_method(name) do
          @serial_attributes_changed = true
          @serial_attributes[name]
        end
      end

      define_method(:"#{name}=") do |value|
        @serial_attributes_changed = true
        @serial_attributes[name] = value
      end
    end

    # Declare a counter. All the counters are internally stored in
    # a different Redis hash, independent from the one that stores
    # the model attributes. Counters are updated with the `incr` and
    # `decr` methods, which interact directly with Redis. Their value
    # can't be assigned as with regular attributes.
    #
    # Example:
    #
    #   class User < Sohm::Model
    #     counter :points
    #   end
    #
    #   u = User.create
    #   u.incr :points
    #
    #   u.points
    #   # => 1
    #
    # Note: You can't use counters until you save the model. If you
    # try to do it, you'll receive an Sohm::MissingID error.
    #
    def self.counter(name)
      counters << name unless counters.include?(name)

      define_method(name) do
        return 0 if new?

        redis.call("HGET", key[:_counters], name).to_i
      end
    end

    # Keep track of `key[name]` and remove when deleting the object.
    def self.track(name)
      tracked << name unless tracked.include?(name)
    end

    # Create a new model, notice that under Sohm's circumstances,
    # this is no longer a syntactic sugar for Model.new(atts).save
    def self.create(atts = {})
      new(atts).save
    end

    # Returns the namespace for the keys generated using this model.
    # Check `Sohm::Model.key` documentation for more details.
    def key
      model.key[id]
    end

    # Initialize a model using a dictionary of attributes.
    #
    # Example:
    #
    #   u = User.new(:name => "John")
    #
    def initialize(atts = {})
      @attributes = {}
      @serial_attributes = {}
      @serial_attributes_changed = false
      update_attributes(atts)
    end

    # Access the ID used to store this model. The ID is used together
    # with the name of the class in order to form the Redis key.
    #
    # Example:
    #
    #   class User < Sohm::Model; end
    #
    #   u = User.create
    #   u.id
    #   # => 1
    #
    #   u.key
    #   # => User:1
    #
    def id
      raise MissingID if not defined?(@id)
      @id
    end

    attr_writer :id
    attr_accessor :cas_token

    # Check for equality by doing the following assertions:
    #
    # 1. That the passed model is of the same type.
    # 2. That they represent the same Redis key.
    #
    def ==(other)
      other.kind_of?(model) && other.key == key
    rescue MissingID
      false
    end

    # Preload all the attributes of this model from Redis. Used
    # internally by `Model::[]`.
    def load!
      update_attributes(Utils.dict(redis.call("HGETALL", key))) if id
      @serial_attributes_changed = false
      return self
    end

    # Returns +true+ if the model is not persisted. Otherwise, returns +false+.
    #
    # Example:
    #
    #   class User < Sohm::Model
    #     attribute :name
    #   end
    #
    #   u = User.new(:name => "John")
    #   u.new?
    #   # => true
    #
    #   u.save
    #   u.new?
    #   # => false
    def new?
      !(defined?(@id) && model.exists?(id))
    end

    # Increment a counter atomically. Internally uses HINCRBY.
    def incr(att, count = 1)
      redis.call("HINCRBY", key[:_counters], att, count)
    end

    # Decrement a counter atomically. Internally uses HINCRBY.
    def decr(att, count = 1)
      incr(att, -count)
    end

    # Return a value that allows the use of models as hash keys.
    #
    # Example:
    #
    #   h = {}
    #
    #   u = User.new
    #
    #   h[:u] = u
    #   h[:u] == u
    #   # => true
    #
    def hash
      new? ? super : key.hash
    end
    alias :eql? :==

    # Returns a hash of the attributes with their names as keys
    # and the values of the attributes as values. It doesn't
    # include the ID of the model.
    #
    # Example:
    #
    #   class User < Sohm::Model
    #     attribute :name
    #   end
    #
    #   u = User.create(:name => "John")
    #   u.attributes
    #   # => { :name => "John" }
    #
    def attributes
      @attributes
    end

    def serial_attributes
      @serial_attributes
    end

    def counters
      hash = {}
      self.class.counters.each do |name|
        hash[name] = 0
      end
      return hash if new?
      redis.call("HGETALL", key[:_counters]).each_slice(2).each do |pair|
        hash[pair[0].to_sym] = pair[1].to_i
      end
      hash
    end

    # Export the ID of the model. The approach of Ohm is to
    # whitelist public attributes, as opposed to exporting each
    # (possibly sensitive) attribute.
    #
    # Example:
    #
    #   class User < Sohm::Model
    #     attribute :name
    #   end
    #
    #   u = User.create(:name => "John")
    #   u.to_hash
    #   # => { :id => "1" }
    #
    # In order to add additional attributes, you can override `to_hash`:
    #
    #   class User < Sohm::Model
    #     attribute :name
    #
    #     def to_hash
    #       super.merge(:name => name)
    #     end
    #   end
    #
    #   u = User.create(:name => "John")
    #   u.to_hash
    #   # => { :id => "1", :name => "John" }
    #
    def to_hash
      attrs = {}
      attrs[:id] = id unless new?

      return attrs
    end


    # Persist the model attributes and update indices and unique
    # indices. The `counter`s and `set`s are not touched during save.
    #
    # Example:
    #
    #   class User < Sohm::Model
    #     attribute :name
    #   end
    #
    #   u = User.new(:name => "John").save
    #   u.kind_of?(User)
    #   # => true
    #
    def save
      if serial_attributes_changed
        response = script(LUA_SAVE, 1, key,
          sanitize_attributes(serial_attributes).to_msgpack,
          cas_token)

        if response.is_a?(RuntimeError)
          if response.message =~ /cas_error/
            raise CasViolation
          else
            raise response
          end
        end

        @cas_token = response
        @serial_attributes_changed = false
      end

      redis.call("HSET", key, "_ndata",
                 sanitize_attributes(attributes).to_msgpack)

      refresh_indices

      return self
    end

    # Delete the model, including all the following keys:
    #
    # - <Model>:<id>
    # - <Model>:<id>:_counters
    # - <Model>:<id>:<set name>
    #
    # If the model has uniques or indices, they're also cleaned up.
    #
    def delete
      memo_key = key["_indices"]
      commands = [["DEL", key], ["DEL", memo_key], ["DEL", key["_counters"]]]
      index_list = redis.call("SMEMBERS", memo_key)
      index_list.each do |index_key|
        commands << ["SREM", index_key, id]
      end
      model.tracked.each do |tracked_key|
        commands << ["DEL", key[tracked_key]]
      end

      model.synchronize do
        commands.each do |command|
          redis.queue(*command)
        end
        redis.commit
      end

      return self
    end

    # Run lua scripts and cache the sha in order to improve
    # successive calls.
    def script(file, *args)
      response = nil

      if Sohm.enable_evalsha
        response = redis.call("EVALSHA", LUA_SAVE_DIGEST, *args)
        if response.is_a?(RuntimeError)
          if response.message =~ /NOSCRIPT/
            response = nil
          end
        end
      end

      response ? response : redis.call("EVAL", LUA_SAVE, *args)
    end

    # Update the model attributes and call save.
    #
    # Example:
    #
    #   User[1].update(:name => "John")
    #
    #   # It's the same as:
    #
    #   u = User[1]
    #   u.update_attributes(:name => "John")
    #   u.save
    #
    def update(attributes)
      update_attributes(attributes)
      save
    end

    # Write the dictionary of key-value pairs to the model.
    def update_attributes(atts)
      unpack_attrs(atts).each { |att, val| send(:"#{att}=", val) }
    end

  protected
    attr_reader :serial_attributes_changed

    def self.to_reference
      name.to_s.
        match(/^(?:.*::)*(.*)$/)[1].
        gsub(/([a-z\d])([A-Z])/, '\1_\2').
        downcase.to_sym
    end

    # Workaround to JRuby's concurrency problem
    def self.inherited(subclass)
      subclass.instance_variable_set(:@indices, [])
      subclass.instance_variable_set(:@counters, [])
      subclass.instance_variable_set(:@tracked, [])
      subclass.instance_variable_set(:@attributes, [])
      subclass.instance_variable_set(:@serial_attributes, [])
    end

    def self.indices
      @indices
    end

    def self.counters
      @counters
    end

    def self.tracked
      @tracked
    end

    def self.attributes
      @attributes
    end

    def self.serial_attributes
      @serial_attributes
    end

    def self.filters(dict)
      unless dict.kind_of?(Hash)
        raise ArgumentError,
          "You need to supply a hash with filters. " +
          "If you want to find by ID, use #{self}[id] instead."
      end

      dict.map { |k, v| to_indices(k, v) }.flatten
    end

    def self.to_indices(att, val)
      raise IndexNotFound unless indices.include?(att)

      if val.kind_of?(Enumerable)
        val.map { |v| key[:_indices][att][v] }
      else
        [key[:_indices][att][val]]
      end
    end

    def fetch_indices
      indices = {}
      model.indices.each { |field| indices[field] = Array(send(field)) }
      indices
    end

    # This is performed asynchronously
    def refresh_indices
      memo_key = key["_indices"]
      # Add new indices first
      commands = fetch_indices.each_pair.map do |field, vals|
        vals.map do |val|
          index_key = model.key["_indices"][field][val]
          [["SADD", memo_key, index_key], ["SADD", index_key, id]]
        end
      end.flatten(2)

      # TODO: Think about switching to a redis pool later
      model.synchronize do
        commands.each do |command|
          redis.queue(*command)
        end
        redis.commit
      end

      # Remove old indices
      # TODO: we can do this asynchronously, or maybe in a background queue
      index_set = ::Set.new(redis.call("SMEMBERS", memo_key))
      valid_list = model[id].send(:fetch_indices).each_pair.map do |field, vals|
        vals.map do |val|
          model.key["_indices"][field][val]
        end
      end.flatten(1)
      valid_set = ::Set.new(valid_list)
      diff_set = index_set - valid_set
      diff_list = diff_set.to_a
      commands = diff_list.map do |key|
        ["SREM", key, id]
      end + [["SREM", memo_key] + diff_list]

      model.synchronize do
        commands.each do |command|
          redis.queue(*command)
        end
        redis.commit
      end
    end

    # Unpack hash returned by redis, which contains _cas, _sdata, _ndata
    # columns
    def unpack_attrs(attrs)
      if ndata = attrs.delete("_ndata")
        attrs.merge!(MessagePack.unpack(ndata))
      end

      if sdata = attrs.delete("_sdata")
        attrs.merge!(MessagePack.unpack(sdata))
      end

      if cas_token = attrs.delete("_cas")
        attrs["cas_token"] = cas_token
      end

      attrs
    end

    def sanitize_attributes(attributes)
      attributes.select { |key, val| val }
    end

    def model
      self.class
    end

    def redis
      model.redis
    end
  end
end
