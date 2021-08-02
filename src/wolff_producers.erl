%%%-------------------------------------------------------------------
%%% @author kevin-T
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. 8月 2021 下午1:54
%%%-------------------------------------------------------------------
-module(wolff_producers).
-author("kevin-T").
-export([start_link/3]).
-export([start_linked_producers/3, stop_linked/1]).
-export([start_supervised/3, stop_supervised/1, stop_supervised/3]).
-export([pick_producer/2, lookup_producer/2]).

-export([code_change/3, handle_call/3, handle_cast/2, handle_info/2, init/1, terminate/2]).
-export_type([producers/0]).

-include("../include/wolff.hrl").

-opaque producers() :: #{
workers := #{partition() => pid()} | ets:tab(),
partition_cnt := pos_integer(),
partitioner := partitioner(),
client => wolff:client_id() | pid(),
topic => kpro:topic()}.

-type topic() :: kpro:topic().
-type partition() :: kpro:partition().
-type config() :: wolff_producer:config().
-type partitioner() :: random|roundrobin|first_key_dispatch|fun((PartitionCount :: pos_integer(), [wolff:msg()]) -> partition()) |partition().


start_link(ClientId, Topic, Config) ->
  Name = get_name(Config),
  gen_server:start_link({local, Name}, wolff_producers, {ClientId, Topic, Config}, []).

-spec start_linked_producers(wolff:client_id() | pid(), topic(), config()) -> {ok, producers()} |{error, any()}.
start_linked_producers(Client, Topic, ProducerCfg) ->
  {ClientId, ClientPid} = case is_binary(Client) of
                            true ->
                              {ok, Pid} = wolff_client_sup:find_client(Client),
                              {Client, Pid};
                            false -> {wolff_client:get_id(Client), Client}
                          end,
  case wolff_client:get_leader_connections(ClientPid, Topic) of
    {ok, Connections} ->
      Workers = start_link_producers(ClientId, Topic, Connections, ProducerCfg),
      Partitioner = maps:get(partitioner, ProducerCfg, random),
      {ok,
        #{client => Client, topic => Topic, workers => Workers,
          partition_cnt => maps:size(Workers),
          partitioner => Partitioner}};
    {error, Reason} -> {error, Reason}
  end.

stop_linked(#{workers := Workers}) when is_map(Workers) ->
  lists:foreach(fun ({_, Pid}) -> wolff_producer:stop(Pid) end, maps:to_list(Workers)).

-spec start_supervised(wolff:client_id(), topic(), config()) -> {ok, producers()}.

start_supervised(ClientId, Topic, ProducerCfg) ->
  {ok, Pid} = wolff_producers_sup:ensure_present(ClientId, Topic, ProducerCfg),
  case gen_server:call(Pid, get_workers, infinity) of
    {0, not_initialized} ->
      {error, failed_to_initialize_producers_in_time};
    {Cnt, Ets} ->
      {ok,
        #{client => ClientId, topic => Topic, workers => Ets,
          partition_cnt => Cnt,
          partitioner =>
          maps:get(partitioner, ProducerCfg, random)}}
  end.

-spec stop_supervised(producers()) -> ok.
stop_supervised(#{client := ClientId,
  workers := NamedEts, topic := Topic}) ->
  stop_supervised(ClientId, Topic, NamedEts).

-spec stop_supervised(wolff:client_id(), topic(), wolff:name()) -> ok.
stop_supervised(ClientId, Topic, NamedEts) ->
  wolff_producers_sup:ensure_absence(ClientId, NamedEts),
  {ok, Pid} = wolff_client_sup:find_client(ClientId),
  ok = wolff_client:delete_producers_metadata(Pid, Topic).

-spec pick_producer(producers(), [wolff:msg()]) -> {partition(), pid()}.
pick_producer(#{workers := Workers, partition_cnt := Count, partitioner := Partitioner}, Batch) ->
  Partition = pick_partition(Count, Partitioner, Batch),
  do_pick_producer(Partitioner,
    Partition,
    Count,
    Workers).

do_pick_producer(Partitioner, Partition, Count,
    Workers) ->
  Pid = lookup_producer(Workers, Partition),
  case is_pid(Pid) andalso is_process_alive(Pid) of
    true -> {Partition, Pid};
    false when Partitioner =:= random ->
      pick_next_alive(Workers, Partition, Count);
    false when Partitioner =:= roundrobin ->
      R = {Partition, Pid} = pick_next_alive(Workers,
        Partition,
        Count),
      _ = put(wolff_roundrobin, (Partition + 1) rem Count),
      R;
    false -> erlang:error({producer_down, Pid})
  end.

pick_next_alive(Workers, Partition, Count) ->
  pick_next_alive(Workers,
    (Partition + 1) rem Count,
    Count,
    _Tried = 1).

pick_next_alive(_Workers, _Partition, Count, Count) ->
  erlang:error(all_producers_down);
pick_next_alive(Workers, Partition, Count, Tried) ->
  Pid = lookup_producer(Workers, Partition),
  case is_alive(Pid) of
    true -> {Partition, Pid};
    false ->
      pick_next_alive(Workers,
        (Partition + 1) rem Count,
        Count,
        Tried + 1)
  end.

is_alive(Pid) ->
  is_pid(Pid) andalso is_process_alive(Pid).

lookup_producer(#{workers := Workers}, Partition) ->
  lookup_producer(Workers, Partition);
lookup_producer(Workers, Partition)
  when is_map(Workers) ->
  maps:get(Partition, Workers);
lookup_producer(Workers, Partition) ->
  [{Partition, Pid}] = ets:lookup(Workers, Partition),
  Pid.

pick_partition(_Count, Partition, _)
  when is_integer(Partition) ->
  Partition;
pick_partition(Count, F, Batch) when is_function(F) ->
  F(Count, Batch);
pick_partition(Count, Partitioner, _)
  when not is_integer(Count); Count =< 0 ->
  error({invalid_partition_count, Count, Partitioner});
pick_partition(Count, random, _) ->
  rand:uniform(Count) - 1;
pick_partition(Count, roundrobin, _) ->
  Partition = case get(wolff_roundrobin) of
                undefined -> 0;
                Number -> Number
              end,
  _ = put(wolff_roundrobin, (Partition + 1) rem Count),
  Partition;
pick_partition(Count, first_key_dispatch,
    [#{key := Key} | _]) ->
  erlang:phash2(Key) rem Count.

init({ClientId, Topic, Config}) ->
  erlang:process_flag(trap_exit, true),
  self() ! rediscover_client,
  {ok,
    #{client_id => ClientId, client_pid => false,
      topic => Topic, config => Config,
      ets => not_initialized, partition_cnt => 0}}.

handle_info(rediscover_client,
    #{client_id := ClientId, client_pid := false} = St0) ->
  St1 = St0#{rediscover_client_tref => false},
  case wolff_client_sup:find_client(ClientId) of
    {ok, Pid} ->
      _ = erlang:monitor(process, Pid),
      St2 = St1#{client_pid := Pid},
      St3 = maybe_init_producers(St2),
      St = maybe_restart_producers(St3),
      {noreply, St};
    {error, Reason} ->
      log_error("Failed to discover client, reason = ~p",
        [Reason]),
      {noreply, ensure_rediscover_client_timer(St1)}
  end;
handle_info(init_producers, St) ->
  {noreply, maybe_init_producers(St)};
handle_info({'DOWN', _, process, Pid, Reason},
    #{client_id := ClientId, client_pid := Pid} = St) ->
  log_error("Client ~p (pid = ~p) down, reason: ~p",
    [ClientId, Pid, Reason]),
  {noreply,
    ensure_rediscover_client_timer(St#{client_pid :=
    false})};
handle_info({'EXIT', Pid, Reason},
    #{ets := Ets, topic := Topic, client_id := ClientId,
      client_pid := ClientPid, config := Config} =
      St) ->
  case ets:match(Ets, {'$1', Pid}) of
    [] ->
      log_error("Unknown EXIT message of pid ~p reason: ~p",
        [Pid, Reason]);
    [[Partition]] ->
      case is_alive(ClientPid) of
        true ->
          log_error("Producer ~s-~p (pid = ~p) down\nreason: ~p",
            [Topic, Partition, Pid, Reason]),
          ok = start_producer_and_insert_pid(Ets,
            ClientId,
            Topic,
            Partition,
            Config);
        false -> ets:insert(Ets, {Partition, {down, Reason}})
      end
  end,
  {noreply, St};
handle_info(Info, St) ->
  log_error("Unknown info ~p", [Info]),
  {noreply, St}.

handle_call(get_workers, _From,
    #{ets := Ets, partition_cnt := Cnt} = St) ->
  {reply, {Cnt, Ets}, St};
handle_call(Call, From, St) ->
  log_error("Unknown call ~p from ~p", [Call, From]),
  {reply, {error, unknown_call}, St}.

handle_cast(Cast, St) ->
  log_error("Unknown cast ~p", [Cast]),
  {noreply, St}.

code_change(_OldVsn, St, _Extra) -> {ok, St}.

terminate(_, _St) -> ok.

ensure_rediscover_client_timer(#{rediscover_client_tref := false} = St) ->
  Tref = erlang:send_after(1000,
    self(),
    rediscover_client),
  St#{rediscover_client_tref := Tref}.

log_error(Fmt, Args) ->
  error_logger:error_msg(Fmt, Args).

start_link_producers(ClientId, Topic, Connections,
    Config) ->
  lists:foldl(fun ({Partition, MaybeConnPid}, Acc) ->
    {ok, WorkerPid} = wolff_producer:start_link(ClientId,
      Topic,
      Partition,
      MaybeConnPid,
      Config),
    Acc#{Partition => WorkerPid}
              end,
    #{},
    Connections).

maybe_init_producers(#{ets := not_initialized,
  topic := Topic, client_id := ClientId,
  config := Config} =
  St) ->
  case start_linked_producers(ClientId, Topic, Config) of
    {ok, #{workers := Workers}} ->
      Ets = ets:new(get_name(Config),
        [protected, named_table, {read_concurrency, true}]),
      true = ets:insert(Ets, maps:to_list(Workers)),
      St#{ets := Ets, partition_cnt => maps:size(Workers)};
    {error, Reason} ->
      log_error("Failed to init producers for topic ~s, "
      "reason: ~p",
        [Topic, Reason]),
      erlang:send_after(1000, self(), init_producers),
      St
  end;
maybe_init_producers(St) -> St.

maybe_restart_producers(#{ets := not_initialized} =
  St) ->
  St;
maybe_restart_producers(#{ets := Ets,
  client_id := ClientId, topic := Topic,
  config := Config} =
  St) ->
  lists:foreach(fun ({Partition, Pid}) ->
    case is_alive(Pid) of
      true -> ok;
      false ->
        start_producer_and_insert_pid(Ets,
          ClientId,
          Topic,
          Partition,
          Config)
    end
                end,
    ets:tab2list(Ets)),
  St.

get_name(Config) ->
  maps:get(name, Config, wolff_producers).

start_producer_and_insert_pid(Ets, ClientId, Topic,
    Partition, Config) ->
  {ok, Pid} = wolff_producer:start_link(ClientId,
    Topic,
    Partition,
    {down, to_be_discovered},
    Config),
  ets:insert(Ets, {Partition, Pid}),
  ok.

