%%%-------------------------------------------------------------------
%%% @author kevin-T
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. 8月 2021 下午1:54
%%%-------------------------------------------------------------------
-module(wolff_stats).
-author("kevin-T").

-behaviour(gen_server).

-export([start_link/0, recv/4, sent/4,getstat/0,getstat/3]).

-export([code_change/3, handle_call/3, handle_cast/2, handle_info/2, init/1, terminate/2]).

start_link() ->
  gen_server:start_link({local, wolff_stats}, wolff_stats, [], []).

recv(ClientId, Topic, Partition, #{cnt := Cnt, oct := Oct} = Numbers) ->
  ok = bump_counter({recv_cnt,
    ClientId,
    Topic,
    Partition},
    Cnt),
  ok = bump_counter({recv_oct,
    ClientId,
    Topic,
    Partition},
    Oct),
  gen_server:cast(wolff_stats, {recv, Numbers}).

sent(ClientId, Topic, Partition, #{cnt := Cnt, oct := Oct} = Numbers) ->
  ok = bump_counter({send_cnt,
    ClientId,
    Topic,
    Partition},
    Cnt),
  ok = bump_counter({send_oct,
    ClientId,
    Topic,
    Partition},
    Oct),
  gen_server:cast(wolff_stats, {sent, Numbers}).

getstat() ->
  gen_server:call(wolff_stats, getstat, infinity).

getstat(ClientId, Topic, Partition) ->
  #{send_cnt =>
  get_counter({send_cnt, ClientId, Topic, Partition}),
    send_oct =>
    get_counter({send_oct, ClientId, Topic, Partition}),
    recv_cnt =>
    get_counter({recv_cnt, ClientId, Topic, Partition}),
    recv_oct =>
    get_counter({recv_oct, ClientId, Topic, Partition})}.

init([]) ->
  {ok,
    #{ets =>
    ets:new(wolff_stats,
      [named_table, public, {write_concurrency, true}]),
      send_cnt => 0, send_oct => 0, recv_cnt => 0,
      recv_oct => 0}}.

handle_call(getstat, _From, St) ->
  Result = maps:with([send_cnt,
    send_oct,
    recv_cnt,
    recv_oct],
    St),
  {reply, Result, St};
handle_call(_Call, _From, St) -> {noreply, St}.

handle_cast({recv, Numbers},
    #{recv_oct := TotalOct, recv_cnt := TotalCnt} = St) ->
  #{cnt := Cnt, oct := Oct} = Numbers,
  {noreply,
    St#{recv_oct := TotalOct + Oct,
      recv_cnt := TotalCnt + Cnt}};
handle_cast({sent, Numbers},
    #{send_oct := TotalOct, send_cnt := TotalCnt} = St) ->
  #{cnt := Cnt, oct := Oct} = Numbers,
  {noreply,
    St#{send_oct := TotalOct + Oct,
      send_cnt := TotalCnt + Cnt}};
handle_cast(_Cast, St) -> {noreply, St}.

handle_info(_Info, St) -> {noreply, St}.

code_change(_OldVsn, St, _Extra) -> {ok, St}.

terminate(_Reason, _St) -> ok.

bump_counter(Key, Inc) ->
  try _ = ets:update_counter(wolff_stats,
    Key,
    Inc,
    {Key, 0}),
  ok
  catch
    _:_ -> ok
  end.

get_counter(Key) ->
  case ets:lookup(wolff_stats, Key) of
    [] -> 0;
    [{_, Value}] -> Value
  end.

