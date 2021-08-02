%%%-------------------------------------------------------------------
%%% @author kevin-T
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. 8月 2021 下午1:54
%%%-------------------------------------------------------------------
-module(wolff_producers_sup).
-author("root").

-behaviour(supervisor).
-export([start_link/0, init/1]).
-export([ensure_present/3, ensure_absence/2]).

start_link() ->
  supervisor:start_link({local,wolff_producers_sup},wolff_producers_sup, []).

init([]) ->
  SupFlags = #{strategy => one_for_one, intensity => 10, period => 5},
  Children = [],
  {ok, {SupFlags, Children}}.

-spec ensure_present(wolff:client_id(), kpro:topic(), wolff_producer:config()) -> {ok, pid()} |{error, client_not_running}.
ensure_present(ClientId, Topic, Config) ->
  ChildSpec = child_spec(ClientId, Topic, Config),
  case supervisor:start_child(wolff_producers_sup,ChildSpec) of
    {ok, Pid} -> {ok, Pid};
    {error, {already_started, Pid}} -> {ok, Pid};
    {error, already_present} -> {error, not_running}
  end.

-spec ensure_absence(wolff:client_id(), wolff:name()) -> ok.
ensure_absence(ClientId, Name) ->
  Id = {ClientId, Name},
  case supervisor:terminate_child(wolff_producers_sup, Id)
  of
    ok ->
      ok = supervisor:delete_child(wolff_producers_sup, Id);
    {error, not_found} -> ok
  end.

child_spec(ClientId, Topic, Config) ->
  #{id => {ClientId, get_name(Config)},
    start => {wolff_producers, start_link, [ClientId, Topic, Config]},
    restart => transient, type => worker,
    modules => [wolff_producers]}.

get_name(Config) ->
  maps:get(name, Config, wolff_producers).

