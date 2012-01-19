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
           %% ping
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
                ok = file:delete(Filename),
                %% file:delete(Filename ++ ".out"),
                true
            end).

%% Generators
bucket() ->
    noshrink(elements(["b", "b1", "b2","b3", "b4", "b5"])).

key() ->
    noshrink(?LET(I, int(), integer_to_list(I))).

value() ->
    binary().

extract_command({set,_,Cmd}) ->
    Cmd.
