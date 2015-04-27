module Sohm
  module AutoId
    def save
      begin
        id
      rescue MissingID
        self.id = redis.call("INCR", model.key["_id"]).to_s
      end

      super
    end
  end
end
