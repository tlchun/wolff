%%%-------------------------------------------------------------------
%%% @author kevin-T
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. 8月 2021 下午1:53
%%%-------------------------------------------------------------------
-module(wolff).
-author("kevin-T").


-export([ensure_supervised_client/3, stop_and_delete_supervised_client/1]).
-export([start_producers/3, stop_producers/1]).
-export([ensure_supervised_producers/3, stop_and_delete_supervised_producers/1, stop_and_delete_supervised_producers/3]).

-export([send/3, send_sync/3]).
-export([get_producer/2]).

-export_type([client_id/0,
  host/0,
  producers/0,
  msg/0,
  ack_fun/0,
  partitioner/0,
  name/0,
  offset_reply/0]).

-type client_id() :: binary().
-type host() :: kpro:endpoint().
-type topic() :: kpro:topic().
-type partition() :: kpro:partition().
-type name() :: atom().
-type offset() :: kpro:offset().

-type offset_reply() :: offset() | buffer_overflow_discarded.
-type producer_cfg() :: wolff_producer:config().
-type producers() :: wolff_producers:producers().
-type partitioner() :: wolff_producers:partitioner().
-type msg() :: #{key := binary(), value := binary(), ts => pos_integer(), headers => [{binary(), binary()}]}.
-type ack_fun() :: fun((partition(), offset_reply()) -> ok) |{fun(), [term()]}.

-spec ensure_supervised_client(client_id(), [host()], wolff_client:config()) -> {ok, pid()} |{error, any()}.
ensure_supervised_client(ClientId, Hosts, Config) ->
  wolff_client_sup:ensure_present(ClientId,
    Hosts,
    Config).

-spec stop_and_delete_supervised_client(client_id()) -> ok.
stop_and_delete_supervised_client(ClientId) ->
  wolff_client_sup:ensure_absence(ClientId).

-spec start_producers(pid(), topic(), producer_cfg()) -> {ok, producers()} | {error, any()}.
start_producers(Client, Topic, ProducerCfg) when is_pid(Client) ->
  wolff_producers:start_linked_producers(Client, Topic, ProducerCfg).

-spec stop_producers(#{workers := map(), _ => _}) -> ok.
stop_producers(Producers) -> wolff_producers:stop_linked(Producers).

-spec ensure_supervised_producers(client_id(), topic(), producer_cfg()) -> {ok, producers()} |{error, any()}.
ensure_supervised_producers(ClientId, Topic, ProducerCfg) ->
  wolff_producers:start_supervised(ClientId, Topic, ProducerCfg).

-spec stop_and_delete_supervised_producers(client_id(), topic(), name()) -> ok.
stop_and_delete_supervised_producers(ClientId, Topic, Name) ->
  wolff_producers:stop_supervised(ClientId, Topic, Name).

-spec stop_and_delete_supervised_producers(wolff_producers:producers()) -> ok.
stop_and_delete_supervised_producers(Producers) ->
  wolff_producers:stop_supervised(Producers).

-spec send(producers(), [msg()], ack_fun()) -> {partition(), pid()}.
send(Producers, Batch, AckFun) ->
  {Partition, ProducerPid} = wolff_producers:pick_producer(Producers, Batch),
  ok = wolff_producer:send(ProducerPid, Batch, AckFun),
  {Partition, ProducerPid}.

-spec send_sync(producers(), [msg()], timeout()) -> {partition(), offset_reply()}.
send_sync(Producers, Batch, Timeout) ->
  {_Partition, ProducerPid} = wolff_producers:pick_producer(Producers, Batch),
  wolff_producer:send_sync(ProducerPid, Batch, Timeout).

get_producer(Producers, Partition) ->
  wolff_producers:lookup_producer(Producers, Partition).
