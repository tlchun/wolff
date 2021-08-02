%%%-------------------------------------------------------------------
%%% @author kevin-T
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. 8月 2021 下午1:53
%%%-------------------------------------------------------------------
-module(wolff_client).
-author("root").
-include("../include/kpro.hrl").
-include("../include/kpro_public.hrl").
-include("../include/kpro_error_codes.hrl").
-include("../include/wolff.hrl").


-export([start_link/3, stop/1]).

-export([get_leader_connections/2,
  recv_leader_connection/4,
  get_id/1,
  delete_producers_metadata/2]).

-export([code_change/3,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  init/1,
  terminate/2]).

-export_type([config/0]).

-type config() :: map().
-type topic() :: kpro:topic().
-type partition() :: kpro:partition().
-type connection() :: kpro:connection().
-type host() :: wolff:host().
-type conn_id() :: {topic(), partition()} | host().

-type state() :: #{client_id := wolff:client_id(),
seed_hosts := host(), config := config(),
connect := fun((host()) -> {ok, connection()} | {error, any()}),
conns := #{conn_id() => connection()},
metadata_ts := #{topic() => erlang:timestamp()},
leaders => #{{topic(), partition()} => connection()}}.

-spec start_link(wolff:client_id(), [host()], config()) -> {ok, pid()} | {error, any()}.
start_link(ClientId, Hosts, Config) ->
  {ConnCfg0, MyCfg} = split_config(Config),
  ConnCfg = ConnCfg0#{client_id => ClientId},
  St = #{client_id => ClientId, seed_hosts => Hosts,
    config => MyCfg,
    connect =>
    fun (Host) -> kpro:connect(Host, ConnCfg) end,
    conns => #{}, metadata_ts => #{}, leaders => #{}},
  case maps:get(reg_name, Config, false) of
    false -> gen_server:start_link(wolff_client, St, []);
    Name ->
      gen_server:start_link({local, Name},
        wolff_client,
        St,
        [])
  end.

stop(Pid) -> gen_server:call(Pid, stop, infinity).

get_id(Pid) -> gen_server:call(Pid, get_id, infinity).

get_leader_connections(Client, Topic) ->
  gen_server:call(Client,
    {get_leader_connections, Topic},
    infinity).

recv_leader_connection(Client, Topic, Partition, Pid) ->
  gen_server:cast(Client,
    {recv_leader_connection, Topic, Partition, Pid}).

delete_producers_metadata(Client, Topic) ->
  gen_server:cast(Client,
    {delete_producers_metadata, Topic}).

init(St) ->
  erlang:process_flag(trap_exit, true),
  {ok, St}.

handle_call(get_id, _From, #{client_id := Id} = St) ->
  {reply, Id, St};
handle_call({get_leader_connections, Topic}, _From,
    St0) ->
  case ensure_leader_connections(St0, Topic) of
    {ok, St} ->
      Result = do_get_leader_connections(St, Topic),
      {reply, {ok, Result}, St};
    {error, Reason} -> {reply, {error, Reason}, St0}
  end;
handle_call(stop, From, #{conns := Conns} = St) ->
  ok = close_connections(Conns),
  gen_server:reply(From, ok),
  {stop, normal, St#{conns := #{}}};
handle_call(_Call, _From, St) -> {noreply, St}.

handle_info(_Info, St) -> {noreply, St}.

handle_cast({recv_leader_connection,
  Topic,
  Partition,
  Caller},
    St0) ->
  case ensure_leader_connections(St0, Topic) of
    {ok, St} ->
      Partitions = do_get_leader_connections(St, Topic),
      {_, MaybePid} = lists:keyfind(Partition, 1, Partitions),
      _ = erlang:send(Caller, {leader_connection, MaybePid}),
      {noreply, St};
    {error, Reason} ->
      _ = erlang:send(Caller,
        {leader_connection, {error, Reason}}),
      {noreply, St0}
  end;
handle_cast({delete_producers_metadata, Topic},
    #{metadata_ts := Topics, conns := Conns} = St) ->
  Conns1 = maps:without([K
    || K = {K1, _} <- maps:keys(Conns), K1 =:= Topic],
    Conns),
  {noreply,
    St#{metadata_ts => maps:remove(Topic, Topics),
      conns => Conns1}};
handle_cast(_Cast, St) -> {noreply, St}.

code_change(_OldVsn, St, _Extra) -> {ok, St}.

terminate(_, #{conns := Conns} = St) ->
  ok = close_connections(Conns),
  {ok, St#{conns := #{}}}.

close_connections(Conns) ->
  lists:foreach(fun ({_, Pid}) -> close_connection(Pid)
                end,
    maps:to_list(Conns)).

close_connection(Conn) ->
  _ = spawn(fun () -> do_close_connection(Conn) end),
  ok.

do_close_connection(Pid) ->
  Mref = erlang:monitor(process, Pid),
  erlang:send(Pid, {{self(), Mref}, stop}),
  receive
    {Mref, Reply} ->
      erlang:demonitor(Mref, [flush]),
      Reply;
    {'DOWN', Mref, _, _, Reason} ->
      {error, {connection_down, Reason}}
  after 5000 -> exit(Pid, kill)
  end.

do_get_leader_connections(#{conns := Conns} = St,
    Topic) ->
  FindInMap = case get_connection_strategy(St) of
                per_partition -> Conns;
                per_broker -> maps:get(leaders, St)
              end,
  F = fun ({T, P}, MaybePid, Acc) when T =:= Topic ->
    case is_alive(MaybePid) of
      true -> [{P, MaybePid} | Acc];
      false -> [{P, {down, MaybePid}} | Acc]
    end;
    (_, _, Acc) -> Acc
      end,
  maps:fold(F, [], FindInMap).

is_metadata_fresh(#{metadata_ts := Topics,
  config := Config},
    Topic) ->
  MinInterval = maps:get(min_metadata_refresh_interval,
    Config,
    1000),
  case maps:get(Topic, Topics, false) of
    false -> false;
    Ts ->
      timer:now_diff(erlang:timestamp(), Ts) <
        MinInterval * 1000
  end.

-spec ensure_leader_connections(state(),
    topic()) -> {ok, state()} | {error, any()}.

ensure_leader_connections(St, Topic) ->
  case is_metadata_fresh(St, Topic) of
    true -> {ok, St};
    false -> do_ensure_leader_connections(St, Topic)
  end.

do_ensure_leader_connections(#{connect := ConnectFun,
  seed_hosts := SeedHosts,
  metadata_ts := MetadataTs} =
  St0,
    Topic) ->
  case get_metadata(SeedHosts, ConnectFun, Topic, []) of
    {ok, {Brokers, Partitions}} ->
      St = lists:foldl(fun (Partition, StIn) ->
        try ensure_leader_connection(StIn,
          Brokers,
          Topic,
          Partition)
        catch
          error:Reason ->
            log_warn("Bad metadata for ~p-~p\nreason=~p",
              [Topic,
                Partition,
                Reason]),
            StIn
        end
                       end,
        St0,
        Partitions),
      {ok,
        St#{metadata_ts :=
        MetadataTs#{Topic => erlang:timestamp()}}};
    {error, Reason} ->
      log_warn("Failed to get metadata\nreason: ~p",
        [Reason]),
      {error, failed_to_fetch_metadata}
  end.

ensure_leader_connection(#{connect := ConnectFun,
  conns := Connections0} =
  St0,
    Brokers, Topic, P_Meta) ->
  Leaders0 = maps:get(leaders, St0, #{}),
  ErrorCode = kpro:find(error_code, P_Meta),
  ErrorCode =:= no_error orelse erlang:error(ErrorCode),
  PartitionNum = kpro:find(partition, P_Meta),
  LeaderBrokerId = kpro:find(leader, P_Meta),
  {_, Host} = lists:keyfind(LeaderBrokerId, 1, Brokers),
  Strategy = get_connection_strategy(St0),
  ConnId = case Strategy of
             per_partition -> {Topic, PartitionNum};
             per_broker -> Host
           end,
  Connections = case get_connected(ConnId,
    Host,
    Connections0)
                of
                  already_connected -> Connections0;
                  {needs_reconnect, OldConn} ->
                    ok = close_connection(OldConn),
                    add_conn(ConnectFun(Host), ConnId, Connections0);
                  false ->
                    add_conn(ConnectFun(Host), ConnId, Connections0)
                end,
  St = St0#{conns := Connections},
  case Strategy of
    per_broker ->
      Leaders = Leaders0#{{Topic, PartitionNum} =>
      maps:get(ConnId, Connections)},
      St#{leaders => Leaders};
    _ -> St
  end.

get_connection_strategy(#{config := Config}) ->
  maps:get(connection_strategy, Config, per_partition).

get_connected(Host, Host, Conns) ->
  Pid = maps:get(Host, Conns, false),
  is_alive(Pid) andalso already_connected;
get_connected(ConnId, Host, Conns) ->
  Pid = maps:get(ConnId, Conns, false),
  case is_connected(Pid, Host) of
    true -> already_connected;
    false when is_pid(Pid) -> {needs_reconnect, Pid};
    false -> false
  end.

is_connected(MaybePid, {Host, Port}) ->
  is_alive(MaybePid) andalso
    case kpro_connection:get_endpoint(MaybePid) of
      {ok, {Host1, Port}} ->
        iolist_to_binary(Host) =:= iolist_to_binary(Host1);
      _ -> false
    end.

is_alive(Pid) ->
  is_pid(Pid) andalso erlang:is_process_alive(Pid).

add_conn({ok, Pid}, ConnId, Conns) ->
  Conns#{ConnId => Pid};
add_conn({error, Reason}, ConnId, Conns) ->
  Conns#{ConnId => Reason}.

split_config(Config) ->
  ConnCfgKeys = kpro_connection:all_cfg_keys(),
  Pred = fun ({K, _V}) -> lists:member(K, ConnCfgKeys)
         end,
  {ConnCfg, MyCfg} = lists:partition(Pred,
    maps:to_list(Config)),
  {maps:from_list(ConnCfg), maps:from_list(MyCfg)}.

get_metadata([], _ConnectFun, _Topic, Errors) ->
  {error, Errors};
get_metadata([Host | Rest], ConnectFun, Topic,
    Errors) ->
  case ConnectFun(Host) of
    {ok, Pid} ->
      try {ok, Vsns} = kpro:get_api_versions(Pid),
      {_, Vsn} = maps:get(metadata, Vsns),
      do_get_metadata(Vsn, Pid, Topic)
      after
        _ = close_connection(Pid)
      end;
    {error, Reason} ->
      get_metadata(Rest,
        ConnectFun,
        Topic,
        [{Host, Reason} | Errors])
  end.

do_get_metadata(Vsn, Connection, Topic) ->
  Req = kpro:make_request(metadata,Vsn,[{topics, [Topic]}, {allow_auto_topic_creation, false}]),
  case kpro:request_sync(Connection, Req, 10000) of
    {ok, #kpro_rsp{msg = Meta}} ->
      BrokersMeta = kpro:find(brokers, Meta),
      Brokers = [parse_broker_meta(M) || M <- BrokersMeta],
      [TopicMeta] = kpro:find(topic_metadata, Meta),
      ErrorCode = kpro:find(error_code, TopicMeta),
      Partitions = kpro:find(partition_metadata, TopicMeta),
      case ErrorCode =:= no_error of
        true -> {ok, {Brokers, Partitions}};
        false -> {error, ErrorCode}
      end;
    {error, Reason} -> {error, Reason}
  end.

-spec parse_broker_meta(kpro:struct()) -> {integer(), host()}.
parse_broker_meta(BrokerMeta) ->
  BrokerId = kpro:find(node_id, BrokerMeta),
  Host = kpro:find(host, BrokerMeta),
  Port = kpro:find(port, BrokerMeta),
  {BrokerId, {Host, Port}}.

log_warn(Fmt, Args) ->
  error_logger:warning_msg(Fmt, Args).

