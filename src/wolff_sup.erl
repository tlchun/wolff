%%%-------------------------------------------------------------------
%%% @author kevin-T
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. 8月 2021 下午1:54
%%%-------------------------------------------------------------------
-module(wolff_sup).
-author("root").

-behaviour(supervisor).
-export([start_link/0, init/1]).


start_link() ->
  supervisor:start_link({local, wolff_sup}, wolff_sup, []).

init([]) ->
  SupFlags = #{strategy => one_for_all, intensity => 10, period => 5},
  Children = [stats_worker(), client_sup(), producers_sup()],
  {ok, {SupFlags, Children}}.

stats_worker() ->
  #{id => wolff_stats,
    start => {wolff_stats, start_link, []},
    restart => permanent, shutdown => 2000, type => worker,
    modules => [wolff_stats]}.

client_sup() ->
  #{id => wolff_client_sup,
    start => {wolff_client_sup, start_link, []},
    restart => permanent, shutdown => 5000,
    type => supervisor, modules => [wolff_client_sup]}.

producers_sup() ->
  #{id => wolff_producers_sup,
    start => {wolff_producers_sup, start_link, []},
    restart => permanent, shutdown => 5000,
    type => supervisor, modules => [wolff_producers_sup]}.
