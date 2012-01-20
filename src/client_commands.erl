-module(client_commands).

-compile(export_all).

write_commands(Filename, Commands) ->
    Encoded = json2:encode(to_json(Commands)),
    file:write_file(Filename, Encoded).

to_json(List) when is_list(List) ->
    [to_json(C) || C <- List];
to_json(ping) ->
    {struct, [{command, ping}]};
to_json({get,B,K}) ->
    {struct, [{command, get}] ++
         encode_bkey(B,K)};
to_json({put,B,K,V}) ->
    {struct, [{command, put},
              {value, base64:encode(V)}] ++ 
         encode_bkey(B,K)};
to_json({delete,B,K}) ->
    {struct, [{command, delete}] ++ 
         encode_bkey(B,K)};
to_json({keys,B}) ->
    {struct, [{command, keys}] ++
         encode_bucket(B)}.

encode_bkey(B,K) ->
    encode_bucket(B) ++ [{key, base64:encode(K)}].

encode_bucket(B) ->
    [{bucket, base64:encode(B)}].
