-module(client_commands).

-compile(export_all).

-define(CLIENTS, [ruby,java,python]).
-define(CLIENT_ROOT(C), filename:flatten([?CLIENTS_ROOT, "/", C])).
-define(CLIENTS_ROOT,
        filename:absname("clients", filename:dirname(filename:dirname(code:which(?MODULE))))
       ).

invoke_all_clients(Filename) ->
    [ {C, invoke_client(C, Filename)} || C <- ?CLIENTS ].

invoke_client(ruby, Filename) ->
    Filename1 = filename:absname(Filename),
    ResultsFile = Filename1 ++ ".ruby.out",
    Output = within_dir(?CLIENT_ROOT(ruby),
                        fun() ->
                                Command = lists:flatten(["bundle exec ruby runner.rb -f ",
                                                         Filename1,
                                                         " -o ", ResultsFile,
                                                         " -h 127.0.0.1:8098:8087"]),
                                os:cmd(Command)
                        end),
    case file:consult(ResultsFile) of
        {ok, Results} ->
            ok = file:delete(ResultsFile),
            {Output, Results};
        {error, Error} ->
            {Output, [{error, Error}]}
    end;
invoke_client(java, Filename) ->
    Filename1 = filename:absname(Filename),
    ResultsFile = Filename1 ++ ".java.out",
    Output = within_dir(?CLIENT_ROOT(java),
                        fun() ->
                                Command = lists:flatten(["java -jar target/riak_client_test-1.0-SNAPSHOT-jar-with-dependencies.jar -f ",
                                                         Filename1,
                                                         " -o ", ResultsFile,
                                                         " -h 127.0.0.1:8098:8087"]),
                                os:cmd(Command)
                        end),
    case file:consult(ResultsFile) of
        {ok, Results} ->
            ok = file:delete(ResultsFile),
            {Output, Results};
        {error, Error} ->
            {Output, [{error, Error}]}
    end;
invoke_client(python, Filename) ->
    Filename1 = filename:absname(Filename),
    ResultsFile = Filename1 ++ ".python.out",
    Output = within_dir(?CLIENT_ROOT(python),
                        fun() ->
                                Command = lists:flatten(["python runner.py -f ",
                                                         Filename1,
                                                         " -o ", ResultsFile,
                                                         " -h 127.0.0.1:8098:8087"]),
                                os:cmd(Command)
                        end),
    case file:consult(ResultsFile) of
        {ok, Results} ->
            ok = file:delete(ResultsFile),
            {Output, Results};
        {error, Error} ->
            {Output, [{error, Error}]}
    end.

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

within_dir(Dir, Fun) ->
    {ok, OldDir} = file:get_cwd(),
    ok = file:set_cwd(Dir),
    Result = Fun(),
    ok = file:set_cwd(OldDir),
    Result.
