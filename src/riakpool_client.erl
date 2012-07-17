%% @author David Weldon
%% @author Christian Dahlqvist - WhiteNode Software Ltd
%% @doc riakpool_client is a collection of convenience functions for using
%% riakpool.

-module(riakpool_client).
-export([delete/2, get/2, list_keys/1, put/3]).

%% @spec delete(binary(), binary()) -> ok
%% @doc Delete `Key' from `Bucket'.
delete(Bucket, Key) ->
    riakpool:execute(fun(C) -> riakc_pb_socket:delete(C, Bucket, Key) end), ok.

%% @spec get(binary(), binary()) -> binary() | {error, any()}
%% @doc Returns the value associated with `Key' in `Bucket' as `binary()'.
%% If an error was encountered or the value was not present, returns
%% `{error, any()}'.
get(Bucket, Key) ->
    Fun =
        fun(C) ->
            case riakc_pb_socket:get(C, Bucket, Key) of
                {ok, O} -> riakc_obj:get_value(O);
                {error, E} -> {error, E}
            end
        end,
    riakpool:execute(Fun).

%% @spec list_keys(binary()) -> list() | {error, any()}
%% @doc Returns the list of keys in `Bucket' as `list()'. If an error was
%% encountered, returns `{error, any()}'.
list_keys(Bucket) ->
    Fun = fun(C) -> riakc_pb_socket:list_keys(C, Bucket) end,
    case riakpool:execute(Fun) of
        {ok, List} -> List;
        {error, E} -> {error, E}
    end.

%% @spec put(binary(), binary(), binary()) -> ok
%% @doc Associates `Key' with `Value' in `Bucket'. If `Key' already exists in
%% `Bucket', an update will be preformed.
put(Bucket, Key, Value) ->
    Fun =
        fun(C) ->
            case riakc_pb_socket:get(C, Bucket, Key) of
                {ok, O} ->
                    O2 = riakc_obj:update_value(O, Value),
                    riakc_pb_socket:put(C, O2);
                {error, _} ->
                    O = riakc_obj:new(Bucket, Key, Value),
                    riakc_pb_socket:put(C, O)
            end
        end,
    riakpool:execute(Fun), ok.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

execute_client_test() ->
    {B, K, V1, V2} = {<<"groceries">>, <<"mine">>, <<"eggs">>, <<"toast">>},
    riakpool_connection_sup:start_link(),
    riakpool:start_link(),
    ?assertMatch({error, _}, list_keys(B)),
    ?assertMatch({error, _}, get(B, K)),
    riakpool:start_pool(),
    timer:sleep(200),
    ?assertEqual([], list_keys(B)),
    ?assertMatch({error, _}, get(B, K)),
    ?assertEqual(ok, put(B, K, V1)),
    ?assertEqual(V1, get(B, K)),
    ?assertEqual(ok, put(B, K, V2)),
    ?assertEqual(V2, get(B, K)),
    ?assertEqual([K], list_keys(B)),
    ?assertEqual(ok, delete(B, K)),
    ?assertEqual([], list_keys(B)),
    riakpool:stop(),
    ?assertEqual(0, riakpool:count()).
    

-endif.
