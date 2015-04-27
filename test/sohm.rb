# coding: utf-8
require_relative "helper"

class Model < Sohm::Model
  attribute :name
  serial_attribute :score

  index :name
end

# Sohm-specific tests
test "ID is required" do
  assert_raise Sohm::MissingID do
    Model.create
  end
end

test "filtering#sample" do
  Model.create(name: "a", id: 1)
  Model.create(name: "b", id: 2)
  Model.create(name: "b", id: 3)

  assert_equal "1", Model.find(name: "a").sample.id

  assert ["2", "3"].include?(Model.find(name: "b").sample.id)
end

test "serial attributes require cas token to be set" do
  model = Model.new(name: "a", score: 5, id: 1)
  model2 = Model.new(name: "a", score: 5, id: 1)

  model2.save

  assert_raise Sohm::CasViolation do
    model.save
  end

  model3 = Model[1]
  model3.update(score: 6)

  assert_raise Sohm::CasViolation do
    model2.update(score: 7)
  end
end
