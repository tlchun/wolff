%%%-------------------------------------------------------------------
%%% @author kevin-T
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. 8月 2021 下午1:53
%%%-------------------------------------------------------------------
-module(wolff_client_sup).
-author("kevin-T").

-behaviour(supervisor).
-export([start_link/0, init/1]).
-export([ensure_present/3, ensure_absence/1, find_client/1]).


start_link() ->
  supervisor:start_link({local, wolff_client_sup}, wolff_client_sup,[]).

init([]) ->
  SupFlags = #{strategy => one_for_one, intensity => 10, period => 5},
  Children = [],
  {ok, {SupFlags, Children}}.

-spec ensure_present(wolff:client_id(), [wolff:host()], wolff_client:config()) -> {ok, pid()} |{error, client_not_running}.
ensure_present(ClientId, Hosts, Config) ->
  ChildSpec = child_spec(ClientId, Hosts, Config),
  case supervisor:start_child(wolff_client_sup, ChildSpec) of
    {ok, Pid} -> {ok, Pid};
    {error, {already_started, Pid}} -> {ok, Pid};
    {error, already_present} -> {error, client_not_running}
  end.

-spec ensure_absence(wolff:client_id()) -> ok.
ensure_absence(ClientId) ->
  case supervisor:terminate_child(wolff_client_sup,
    ClientId)
  of
    ok ->
      ok = supervisor:delete_child(wolff_client_sup,
        ClientId);
    {error, not_found} -> ok
  end.

-spec find_client(wolff:client_id()) -> {ok, pid()} |{error, any()}.
find_client(ClientId) ->
  Children = supervisor:which_children(wolff_client_sup),
  case lists:keyfind(ClientId, 1, Children) of
    {ClientId, Client, _, _} when is_pid(Client) ->
      {ok, Client};
    {ClientId, Restarting, _, _} -> {error, Restarting};
    false -> erlang:error({no_such_client, ClientId})
  end.

child_spec(ClientId, Hosts, Config) ->
  #{id => ClientId,
    start =>
    {wolff_client, start_link, [ClientId, Hosts, Config]},
    restart => transient, type => worker,
    modules => [wolff_client]}.
