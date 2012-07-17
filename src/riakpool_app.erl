%% @author David Weldon
%% @author Christian Dahlqvist - WhiteNode Software Ltd
%% @hidden

-module(riakpool_app).
-behaviour(application).
-export([start/2, stop/1]).

start(_StartType, _StartArgs) -> riakpool_sup:start_link().

stop(_State) -> ok.
