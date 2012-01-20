%%% @author Sean Cribbs <sean@seanbasho>
%%% @copyright (C) 2012, Sean Cribbs
%%% @doc
%%%
%%% @end
%%% Created : 18 Jan 2012 by Sean Cribbs <sean@seanbasho>

-module(client_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-compile(export_all).

-record(state,{}).

%% Initialize the state
initial_state() ->
    #state{}.

%% Command generator, S is the state
command(S) ->
    oneof([
           ping,
           {get, bucket(), key()},
           {put, bucket(), key(), value()},
           {delete, bucket(), key()},
           {keys, bucket()}
          ]).

%% Next state transformation, S is the current state
next_state(S,_V,_) ->
    S.

%% Precondition, checked before command is added to the command sequence
precondition(_S,_) ->
    true.

%% Postcondition, checked after command has been evaluated
%% OBS: S is the state before next_state(S,_,<command>)
postcondition(_S,{call,_,_,_},_Res) ->
    true.

prop_clients() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                Filename = io_lib:format("~w~w~w", tuple_to_list(now())),
                client_commands:write_commands(Filename,
                                               [extract_command(C) || C <- Cmds]),
                %% Put client invocation here
                %% Ruby
                Command = lists:flatten(["rvm 1.9.3@ripple do ruby /Users/sean/Development/ripple/riak-client/bin/riak-client-congruent -f ",
                                         Filename,
                                         " -h 127.0.0.1:8098:8087"]),
                Output = os:cmd(Command),
                ?WHENFAIL(
                   io:format("Command: ~s~nOutput: ~s~n", [Command, Output]),
                   ?TRAPEXIT(
                      begin
                          {ok, Results} = file:consult(Filename ++ ".out"),
                          ok = file:delete(Filename),
                          ok = file:delete(Filename ++ ".out"),
                          aggregate(
                            aggregate_command_names(Cmds),
                            equals([ Msg || {error, Msg} <- Results ],
                                   [])
                           )
                      end
                     ))
            end).

%% Generators
bucket() ->
    noshrink(elements(["b", "b1", "b2"])).

key() ->
    noshrink(?LET(I, nat(), integer_to_list(I))).

value() ->
    binary().

extract_command({set,_,Cmd}) ->
    Cmd.

aggregate_command_names(Commands) ->
    [ extract_command_name(Cmd) || Cmd <- Commands ].

extract_command_name({set,_,Cmd}) ->
    extract_command_name(Cmd);
extract_command_name(Atom) when is_atom(Atom) ->
    Atom;
extract_command_name(Tuple) when is_tuple(Tuple) ->
    element(1,Tuple).
