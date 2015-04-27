module Sohm
  module IndexAll
    def self.included(model)
      model.index :all
      model.extend ClassMethods
    end

    def all
      "all"
    end

    module ClassMethods
      def all
        find(all: "all")
      end
    end
  end
end
