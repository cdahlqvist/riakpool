%% @author David Weldon
%% @author Christian Dahlqvist - WhiteNode Software Ltd
%% @doc riakpool implements a pool of riak protocol buffer clients. In order to
%% use a connection, a call to {@link execute/1} must be made. This will check
%% out a connection from the pool, use it, and check it back in. This ensures
%% that a given connection can only be in use by one external process at a time.
%% If no existing connections are found, a new one will be established. Note
%% this means the pool will always be the size of the last peak need. The number
%% of connections can be checked with {@link count/0}.
%%
%% Prior to any calls to {@link execute/1}, the pool must be started. This can
%% be accomplished in one of two ways:
%%
%% 1. Before the server is started, set the riakpool application environment
%%    variables `riakpool_host' and `riakpool_port'. This will initiate the pool
%%    with the default parameters unless these are overridden by specifying one
%%    or more of the following parameters: `pool_size', `increment', `max_age'
%%    and `release_delay'.
%%
%% 2. After the server is started, call {@link start_pool/0},
%%    {@link start_pool/2} or {@link start_pool/3}

-module(riakpool).
-behaviour(gen_server).
-export([count/0,
         execute/1,
         start_link/0,
         start_pool/0,
         start_pool/2,
         start_pool/3,
         stop/0]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).
-record(state, {host, port, pool_size, increment, max_age, release_delay, pids, expired, timecounter}).
-define(POOL_PARAM_DEFAULTS, [{pool_size, 5},
                              {increment, 1},
                              {release_delay, 10},
                              {max_age, 600}]).
-define(HOUSEKEEPING_INTERVAL, 1000).

%% @type host() = string() | atom().
%% @type pool_param_name() = pool_size | increment | release_delay | max_age.
%% @type pool_param() = {pool_param_name(), integer()}.
%% @type pool_param_list() = [pool_param()].

%% @spec count() -> integer()
%% @doc Returns the number of connections as seen by the supervisor.
count() ->
    Props = supervisor:count_children(riakpool_connection_sup),
    case proplists:get_value(active, Props) of
        N when is_integer(N) -> N;
        undefined -> 0
    end.

%% @spec execute(Fun) -> Value::any() | {error, any()}
%%       Fun = function(pid())
%% @doc Finds the next available connection pid from the pool and calls
%% `Fun(Pid)'. Returns `Value' if the call was successful, and
%% `{error, any()}' otherwise. If no connection could be found, a new connection
%% will be established.
%% ```
%% > riakpool:execute(fun(C) -> riakc_pb_socket:ping(C) end).
%% pong
%% '''
execute(Fun) ->
    case gen_server:call(?MODULE, check_out) of
        {ok, Pid} ->
            try Fun(Pid)
            catch _:E -> {error, E}
            after gen_server:cast(?MODULE, {check_in, Pid}) end;
        {error, E} -> {error, E}
    end.

%% @spec start_link() -> {ok, pid()} | {error, any()}
%% @doc Starts the server.
start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @spec start_pool() -> ok | {error, any()}
%% @doc Starts a connection pool to a server listening on {"127.0.0.1", 8087}
%% with default settings.
%% Note that a pool can only be started once.
start_pool() -> start_pool("127.0.0.1", 8087, []).

%% @spec start_pool(host(), integer()) -> ok | {error, any()}
%% @doc Starts a connection pool to a server listening on {`Host', `Port'} with
%% default settings.
%% Note that a pool can only be started once.
start_pool(Host, Port) when is_integer(Port) ->
    start_pool(Host, Port, []).
    
%% @spec start_pool(host(), integer(), pool_param_list()) -> ok | {error, any()}
%% @doc Starts a connection pool to a server listening on {`Host', `Port'} with
%% configuration parameters that override the default settings.
%%
%% The following parameters are supported:
%%
%% `pool_size' - Target size of connection pool. [default: 5]
%%
%% `increment' - When the size of the pool needs to be extended, this is the
%%               number of connections that will be added. [default: 1]
%%
%% `max_age' - Indicates the max age of a connection. Once a connection exceeds
%%             this age it will be replaced with a new one. [default: 600]
%%
%% `release_delay' - Once a connection has been idle for this amount of time it
%%                   will be considered for removal if the number of current
%%                   connections exceed the target pool size. [default: 10]
%%
%% Note that a pool can only be started once.
start_pool(Host, Port, Params) when is_integer(Port), is_list(Params) ->
    gen_server:call(?MODULE, {start_pool, Host, Port, Params}).
    
%% @spec stop() -> ok
%% @doc Stops the server.
stop() -> gen_server:cast(?MODULE, stop).

%% @hidden
init([]) ->
    process_flag(trap_exit, true),
    case [application:get_env(P) || P <- [riakpool_host, riakpool_port]] of
        [{ok, Host}, {ok, Port}] when is_integer(Port) ->
            Params = get_environment_variables(),
            {ok, new_state(Host, Port, Params)};
        _ -> {ok, undefined}
    end.

%% @hidden
handle_call({start_pool, Host, Port, Params}, _From, undefined) ->
    case new_state(Host, Port, Params) of
        undefined -> {reply, {error, connection_error}, undefined};
        State -> {reply, ok, State}
    end;
handle_call({start_pool, _Host, _Port, _Params}, _From, State=#state{}) ->
    {reply, {error, pool_already_started}, State};
handle_call(check_out, _From, undefined) ->
    {reply, {error, pool_not_started}, undefined};
handle_call(check_out, _From, State) ->
    case next_pid(State) of
        {ok, Pid, NewState} -> {reply, {ok, Pid}, NewState};
        {error, NewState} -> {reply, {error, connection_error}, NewState}
    end;
handle_call(_Request, _From, State) -> {reply, ok, State}.

%% @hidden
handle_cast({check_in, Pid}, State=#state{pids=Pids, expired = Expired, timecounter = TC}) ->
    case lists:member(Pid, Expired) of
        true ->
            NewExpired = lists:delete(Pid, Expired),
            riakc_pb_socket:stop(Pid),
            NewState = adjust_pool_size(State#state{expired = NewExpired}),
            {noreply, NewState};
        false ->
            NewPids = queue:in_r({Pid, TC}, Pids),
            {noreply, State#state{pids=NewPids}}
    end;
handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) -> {noreply, State}.

%% @hidden
handle_info(timestamp, #state{timecounter = TC} = State) ->
    NewState = perform_housekeeping(State),
    erlang:send_after(1000, self(), timestamp),
    {noreply, NewState#state{timecounter = (TC + 1)}};
handle_info(housekeeping, State) ->
    NewState = perform_housekeeping(State),
    erlang:send_after(?HOUSEKEEPING_INTERVAL, self(), housekeeping),
    {noreply, NewState};
handle_info({expire_pid, Pid}, #state{pids = Pids, expired = Expired} = State) ->
    case {is_process_alive(Pid), is_pid_enqueued(Pids, Pid)} of
        {true, true} ->
            riakc_pb_socket:stop(Pid),
            NewPids = queue:from_list([{P,T} || {P, T} <- queue:to_list(Pids), P =/= Pid]),
            NewState = adjust_pool_size(State#state{pids = NewPids}),
            {noreply, NewState};
        {true, false} ->
            NewExpired = lists:append(Expired, [Pid]),
            {noreply, State#state{expired = NewExpired}};
        {false, true} ->
            NewPids = queue:from_list([{P,T} || {P, T} <- queue:to_list(Pids), P =/= Pid]),
            {noreply, State#state{pids = NewPids}};
        {false, false} ->
            {noreply, State}
    end;
handle_info(_Info, State) -> {noreply, State}.

%% @hidden
terminate(_Reason, undefined) -> ok;
terminate(_Reason, #state{pids=Pids}) ->
    StopFun =
        fun(P) ->
            case is_process_alive(P) of
                true -> riakc_pb_socket:stop(P);
                false -> ok
            end
        end,
    [StopFun(Pid) || {Pid, _} <- queue:to_list(Pids)], ok.

%% @hidden
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% @hidden
new_state(Host, Port, Params) ->
    TS = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
    ValidatedParams = remove_invalid_parameters(Params),
    FinalParams = default_missing_parameters(ValidatedParams),
    PoolSize = get_parameter(FinalParams, pool_size),
    case new_connections(Host, Port, PoolSize) of
        error -> undefined;
        PidList ->
            UpdatedList = [{P, TS} || P <- PidList],
            MaxAge = get_parameter(FinalParams, max_age),
            lists:foreach(fun(P) -> erlang:send_after((MaxAge * 1000), self(), {expire_pid, P}) end, PidList),
            erlang:send_after(1000, self(), timestamp),
            erlang:send_after(?HOUSEKEEPING_INTERVAL, self(), housekeeping),
            #state{host = Host,
                   port = Port,
                   pool_size = PoolSize,
                   increment = get_parameter(FinalParams, increment),
                   max_age = MaxAge,
                   release_delay = get_parameter(FinalParams, release_delay),
                   expired = [],
                   timecounter = TS,
                   pids = queue:from_list(UpdatedList)}
    end.

%% @hidden
is_pid_enqueued(Pids, Pid) ->
    case [P || {P, _} <- queue:to_list(Pids), P == Pid] of
        [] -> false;
        [Pid] -> true
    end.

%% @hidden
default_missing_parameters(Parameters) ->
    Missing = lists:subtract([D || {D, _} <- ?POOL_PARAM_DEFAULTS], [K || {K, _} <- Parameters]),
    lists:append(Parameters, [{P, V} || {P, V} <- ?POOL_PARAM_DEFAULTS, lists:member(P, Missing)]).

%% @hidden
remove_invalid_parameters(Params) ->
    [{P, V} || {P, V} <- Params, (is_integer(V) and (V > 0))].

%% @hidden
get_environment_variables() ->
    get_environment_variables([D || {D, _} <- ?POOL_PARAM_DEFAULTS], []).

%% @hidden
get_environment_variables([], ValueList) ->
    ValueList;
get_environment_variables([Parameter | List], ValueList) ->
    case application:get_env(Parameter) of
        {ok, Value} ->
            NewValueList = lists:append(ValueList, [{parameter, Value}]),
            get_environment_variables(List, NewValueList);
        _ ->
            get_environment_variables(List, ValueList)
    end.

%% @hidden
get_parameter(ParamList, ParamName) ->
    case [V || {P, V} <- ParamList, P == ParamName] of
        [] -> undefined;
        [Value] -> Value
    end.

%% @hidden
new_connections(_Host, _Port, 0, PidList) ->
    PidList;
new_connections(Host, Port, N, PidList) ->
    case supervisor:start_child(riakpool_connection_sup, [Host, Port]) of
        {ok, Pid} when is_pid(Pid) ->
            NewPidList = lists:append(PidList, [Pid]),
            new_connections(Host, Port, (N - 1), NewPidList);
        {ok, Pid, _} when is_pid(Pid) ->
            NewPidList = lists:append(PidList, [Pid]),
            new_connections(Host, Port, (N - 1), NewPidList);
        _ ->
            case PidList of
                [] -> error;
                _ -> PidList
            end
    end.
  
%% @hidden 
new_connections(Host, Port, N) ->
    new_connections(Host, Port, N, []).

%% @hidden
next_pid(#state{host = Host, port = Port, pids = Pids, increment = Increment,
                pool_size = PoolSize, timecounter = TC} = State) ->
    case queue:out(Pids) of
        {{value, {Pid, _}}, NewPids} ->
            case is_process_alive(Pid) of
                true ->
                    {ok, Pid, State#state{pids = NewPids}};
                false -> next_pid(State#state{pids = NewPids})
            end;
        {empty, _} ->
            Inc = max(Increment, (PoolSize - count())),
            case new_connections(Host, Port, Inc) of
                error -> {error, State#state{pids = queue:new()}};
                [Pid | Rest] ->
                    {ok, Pid, State#state{pids = queue:from_list([{P, TC} || P <- Rest])}}
            end
    end.

%% @hidden
perform_housekeeping(#state{pool_size = PS, pids = Pids, timecounter = TC, release_delay = RD} = State) ->
    NewPidList = [{P, T} || {P, T} <- queue:to_list(Pids), is_process_alive(P)],
    Size = count(),
    case (Size > PS) of
        true ->
            Expireable = [{P, T} || {P, T} <- NewPidList, (T < (TC - RD))],            
            case min(length(Expireable), (Size - PS)) of
                0 ->
                    State#state{pids = queue:from_list(NewPidList)};
                N ->
                    {L1, _} = lists:split(N, Expireable),
                    lists:foreach(fun({P,_}) -> riakc_pb_socket:stop(P) end, L1),
                    NewPids = queue:from_list(lists:subtract(NewPidList, L1)),
                    State#state{pids = NewPids}
            end;
        false ->
            State#state{pids = queue:from_list(NewPidList)}
    end.

%% @hidden
adjust_pool_size(#state{host = Host, port = Port, pool_size = PS, timecounter = TC, pids = Pids} = State) ->
    Size = count(),
    case (Size < PS) of
        true ->
            case new_connections(Host, Port, (PS - Size)) of
                error ->
                    NewPids = Pids;
                PidList ->
                    PidList2 = [{P, TC} || P <- PidList],
                    NewPids = queue:from_list(lists:append(PidList2, queue:to_list(Pids)))
            end,
            State#state{pids = NewPids};
        false ->
            State
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

execute_test() ->
    riakpool_connection_sup:start_link(),
    riakpool:start_link(),
    riakpool:start_pool(),
    ?assertEqual(5, count()),
    Fun1 = fun(C) -> riakc_pb_socket:ping(C) end,
    Fun2 = fun(_) -> riakc_pb_socket:ping(1) end,
    ?assertEqual(pong, execute(Fun1)),
    ?assertMatch({error, _}, execute(Fun2)),
    ?assertEqual(pong, execute(Fun1)),
    riakpool:stop(),
    timer:sleep(10),
    ?assertEqual(0, count()).

execute_poolsize_test() ->
    riakpool_connection_sup:start_link(),
    riakpool:start_link(),
    riakpool:start_pool("127.0.0.1", 8087, [{pool_size, 2}]),
    ?assertEqual(2, count()),
    Fun1 = fun(C) -> riakc_pb_socket:ping(C) end,
    Fun2 = fun(_) -> riakc_pb_socket:ping(1) end,
    ?assertEqual(pong, execute(Fun1)),
    ?assertMatch({error, _}, execute(Fun2)),
    ?assertEqual(pong, execute(Fun1)),
    riakpool:stop(),
    timer:sleep(10),
    ?assertEqual(0, count()).
    
execute_connection_reuse_test() ->
    riakpool_connection_sup:start_link(),
    riakpool:start_link(),
    riakpool:start_pool("127.0.0.1", 8087, [{pool_size, 1}]),
    ?assertEqual(1, count()),
    Fun1 = fun(C) -> riakc_pb_socket:ping(C) end,
    Fun2 = fun(C) -> C end,
    Pid1 = execute(Fun2),
    ?assertEqual(pong, execute(Fun1)),
    ?assertEqual(pong, execute(Fun1)),
    Pid2 = execute(Fun2),
    ?assertEqual(Pid1, Pid2),
    riakpool:stop(),
    timer:sleep(10),
    ?assertEqual(0, count()).

execute_error_test() ->
    riakpool:start_link(),
    Fun = fun(C) -> riakc_pb_socket:ping(C) end,
    ?assertEqual({error, pool_not_started}, execute(Fun)),
    riakpool:stop(),
    timer:sleep(10),
    ?assertEqual(0, count()).

start_pool_test() ->
    riakpool_connection_sup:start_link(),
    riakpool:start_link(),
    {H, P} = {"localhost", 8000},
    ?assertEqual({error, connection_error}, riakpool:start_pool(H, P)),
    ?assertEqual(ok, riakpool:start_pool()),
    ?assertEqual({error, pool_already_started}, riakpool:start_pool()),
    riakpool:stop(),
    timer:sleep(10),
    ?assertEqual(0, count()).

execute_pool_resize_test() ->
    riakpool_connection_sup:start_link(),
    riakpool:start_link(),
    riakpool:start_pool("127.0.0.1", 8087, [{pool_size, 1},{release_delay, 1}]),
    ?assertEqual(1, count()),
    Fun1 = fun(C) -> timer:sleep(200), riakc_pb_socket:ping(C) end,
    Fun2 = fun() -> execute(Fun1) end,
    erlang:spawn(Fun2),
    erlang:spawn(Fun2),
    erlang:spawn(Fun2),
    timer:sleep(100),
    ?assertEqual(3, count()),
    timer:sleep(4000),
    ?assertEqual(1, count()),
    riakpool:stop(),
    timer:sleep(10),
    ?assertEqual(0, count()).
    
execute_pool_increment_test() ->
    riakpool_connection_sup:start_link(),
    riakpool:start_link(),
    riakpool:start_pool("127.0.0.1", 8087, [{pool_size, 2},{release_delay, 1},{increment, 4}]),
    ?assertEqual(2, count()),
    Fun1 = fun(C) -> timer:sleep(200), riakc_pb_socket:ping(C) end,
    Fun2 = fun() -> execute(Fun1) end,
    spawn(Fun2),
    spawn(Fun2),
    spawn(Fun2),
    spawn(Fun2),
    timer:sleep(100),
    ?assertEqual(6, count()),
    timer:sleep(3000),
    ?assertEqual(2, count()),
    riakpool:stop(),
    timer:sleep(10),
    ?assertEqual(0, count()).
    
execute_pool_maxage_test() ->
    riakpool_connection_sup:start_link(),
    riakpool:start_link(),
    riakpool:start_pool("127.0.0.1", 8087, [{pool_size, 1},{max_age, 1}]),
    ?assertEqual(1, count()),
    Fun1 = fun(C) -> C end,
    Pid1 = execute(Fun1),
    timer:sleep(3000),
    ?assertEqual(1, count()),
    Pid2 = execute(Fun1),
    ?assertNotEqual(Pid1, Pid2),
    riakpool:stop(),
    timer:sleep(10),
    ?assertEqual(0, count()).

-endif.
