local ctoken = redis.call('HGET', KEYS[1], '_cas')
if (not ctoken) or ctoken == ARGV[2] then
   local ntoken
   if not ctoken then
      ntoken = 1
   else
      ntoken = tonumber(ctoken) + 1
   end
   redis.call('HMSET', KEYS[1], '_sdata', ARGV[1],
              '_cas', ntoken, '_ndata', ARGV[3])
   return ntoken
else
   error('cas_error')
end
