%%%-------------------------------------------------------------------
%%% @author kevin-T
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. 8月 2021 下午1:54
%%%-------------------------------------------------------------------
-module(wolff_producer).
-author("root").
-include("../include/kpro.hrl").
-include("../include/kpro_public.hrl").
-include("../include/kpro_error_codes.hrl").
-include("../include/wolff.hrl").


-export([start_link/5, stop/1, send/3, send_sync/3]).
-export([code_change/3, handle_call/3, handle_cast/2, handle_info/2, init/1, terminate/2]).
-export([queue_item_sizer/1, queue_item_marshaller/1]).
-export([batch_bytes/1, varint_bytes/1]).

-export_type([config/0]).

-type topic() :: kpro:topic().
-type partition() :: kpro:partition().
-type offset_reply() :: wolff:offset_reply().

-type config() :: #{
replayq_dir := string(),
replayq_max_total_bytes => pos_integer(),
replayq_seg_bytes => pos_integer(),
replayq_offload_mode => boolean(),
required_acks => kpro:required_acks(),
ack_timeout => timeout(),
max_batch_bytes => pos_integer(),
max_linger_ms => non_neg_integer(),
max_send_ahead => non_neg_integer(),
compression => kpro:compress_option()}.

-spec start_link(wolff:client_id(), topic(), partition(), pid() | {down, any()}, config()) -> {ok, pid()} |{error, any()}.
start_link(ClientId, Topic, Partition, MaybeConnPid, Config) ->
  St = #{client_id => ClientId, topic => Topic,
    partition => Partition, conn => MaybeConnPid,
    config => use_defaults(Config),
    linger_expire_timer => false},
  gen_server:start_link(wolff_producer, St, []).

stop(Pid) -> gen_server:call(Pid, stop, infinity).

-spec send(pid(), [wolff:msg()], wolff:ack_fun()) -> ok.
send(Pid, [_ | _] = Batch0, AckFun) ->
  Caller = self(),
  Mref = erlang:monitor(process, Pid),
  Batch = ensure_ts(Batch0),
  erlang:send(Pid, {send, {Caller, Mref}, Batch, AckFun}),
  receive
    {Mref, queued} ->
      erlang:demonitor(Mref, [flush]),
      ok;
    {'DOWN', Mref, _, _, Reason} ->
      erlang:error({producer_down, Reason})
  end.

-spec send_sync(pid(), [wolff:msg()], timeout()) -> {partition(), offset_reply()}.

send_sync(Pid, Batch0, Timeout) ->
  Caller = self(),
  Mref = erlang:monitor(process, Pid),
  AckFun = fun (Partition, BaseOffset) ->
    _ = erlang:send(Caller, {Mref, Partition, BaseOffset}),
    ok
           end,
  Batch = ensure_ts(Batch0),
  erlang:send(Pid,{send, no_queued_reply, Batch, AckFun}),
  receive
    {Mref, Partition, BaseOffset} ->
      erlang:demonitor(Mref, [flush]),
      {Partition, BaseOffset};
    {'DOWN', Mref, _, _, Reason} ->
      erlang:error({producer_down, Reason})
  after Timeout ->
    erlang:demonitor(Mref, [flush]), erlang:error(timeout)
  end.

init(St) ->
  erlang:process_flag(trap_exit, true),
  self() ! {do_init, St},
  {ok, #{}}.

do_init(#{client_id := ClientId, conn := Conn, topic := Topic, partition := Partition, config := Config0} = St) ->
  QCfg = case maps:get(replayq_dir, Config0, false) of
           false -> #{mem_only => true};
           BaseDir ->
             Dir = filename:join([BaseDir, Topic, integer_to_list(Partition)]),
             SegBytes = maps:get(replayq_seg_bytes, Config0, 10 * 1024 * 1024),
             Offload = maps:get(replayq_offload_mode, Config0, false),
             #{dir => Dir, seg_bytes => SegBytes, offload => Offload}
         end,
  MaxTotalBytes = maps:get(replayq_max_total_bytes, Config0, 2000000000),
  Q = replayq:open(QCfg#{
    sizer => fun wolff_producer:queue_item_sizer/1,
    marshaller => fun wolff_producer:queue_item_marshaller/1,
    max_total_bytes => MaxTotalBytes}),
  _ = erlang:send(self(), {leader_connection, Conn}),
  Config = maps:without([replayq_dir, replayq_seg_bytes], Config0),
  St#{
    replayq => Q,
    config := Config,
    call_id_base => erlang:system_time(microsecond),
    pending_acks => #{}, sent_reqs => queue:new(),
    sent_reqs_count => 0, conn := undefined,
    client_id => ClientId}.

handle_call(stop, From, St) ->
  gen_server:reply(From, ok),
  {stop, normal, St};
handle_call(_Call, _From, St) -> {noreply, St}.

handle_info({do_init, St0}, _) ->
  St = do_init(St0),
  {noreply, St};
handle_info(linger_expire, St) ->
  {noreply,maybe_send_to_kafka(St#{linger_expire_timer := false})};
handle_info({send, _, Batch, _} = Call, #{client_id := ClientId, topic := Topic, partition := Partition, config := #{max_batch_bytes := Limit}} = St0) ->
  {Calls, Cnt, Oct} = collect_send_calls([Call], 1, batch_bytes(Batch), Limit),
  ok = wolff_stats:recv(ClientId, Topic, Partition, #{cnt => Cnt, oct => Oct}),
  St1 = enqueue_calls(Calls, St0),
  St = maybe_send_to_kafka(St1),
  {noreply, St};
handle_info({msg, Conn, Rsp}, #{conn := Conn} = St0) ->
  try handle_kafka_ack(Rsp, St0) of
    St1 ->
      St = maybe_send_to_kafka(St1),
      {noreply, St}
  catch
    Reason ->
      St = mark_connection_down(St0, Reason),
      {noreply, St}
  end;
handle_info({leader_connection, Conn}, St0) when is_pid(Conn) ->
  _ = erlang:monitor(process, Conn),
  St1 = St0#{reconnect_timer => no_timer, conn := Conn},
  St2 = get_produce_version(St1),
  St3 = resend_sent_reqs(St2),
  St = maybe_send_to_kafka(St3),
  {noreply, St};
handle_info({leader_connection, {down, Reason}}, St0) ->
  St = mark_connection_down(St0#{reconnect_timer => no_timer}, Reason),
  {noreply, St};
handle_info({leader_connection, {error, Reason}}, St0) ->
  St = mark_connection_down(St0#{reconnect_timer => no_timer}, Reason),
  {noreply, St};
handle_info(reconnect, St0) ->
  St = St0#{reconnect_timer => no_timer},
  {noreply, ensure_delayed_reconnect(St)};
handle_info({'DOWN', _, _, Conn, Reason}, #{conn := Conn} = St0) ->
  #{reconnect_timer := no_timer} = St0,
  St = mark_connection_down(St0, Reason),
  {noreply, St};
handle_info({'EXIT', _, Reason}, St) ->
  {stop, Reason, St};
handle_info(_Info, St) -> {noreply, St}.

handle_cast(_Cast, St) -> {noreply, St}.

code_change(_OldVsn, St, _Extra) -> {ok, St}.

terminate(_, #{replayq := Q}) -> ok = replayq:close(Q);
terminate(_, _) -> ok.

ensure_ts(Batch) ->
  lists:map(fun (#{ts := _} = Msg) -> Msg;
    (Msg) -> Msg#{ts => now_ts()}
            end,
    Batch).

make_call_id(Base) ->
  Base + erlang:unique_integer([positive]).

use_defaults(Config) ->
  use_defaults(Config,
    [{required_acks, all_isr},
      {ack_timeout, 10000},
      {max_batch_bytes, 1000000},
      {max_linger_ms, 0},
      {max_send_ahead, 0},
      {compression, no_compression},
      {reconnect_delay_ms, 2000}]).

use_defaults(Config, []) -> Config;
use_defaults(Config, [{K, V} | Rest]) ->
  case maps:is_key(K, Config) of
    true -> use_defaults(Config, Rest);
    false -> use_defaults(Config#{K => V}, Rest)
  end.

resend_sent_reqs(#{sent_reqs := SentReqs, conn := Conn} = St) ->
  F = fun ({Req0, Q_AckRef, Calls}, Acc) ->
        Req = Req0#kpro_req{ref = make_ref()},
        ok = request_async(Conn, Req),
        NewSent = {Req, Q_AckRef, Calls},
        [NewSent | Acc]
      end, NewSentReqs = lists:foldl(F, [], queue:to_list(SentReqs)),
  St#{sent_reqs := queue:from_list(lists:reverse(NewSentReqs))}.

maybe_send_to_kafka(#{conn := Conn, replayq := Q} = St) ->
  case replayq:count(Q) =:= 0 of
    true -> St;
    false when is_pid(Conn) ->
      maybe_send_to_kafka_has_pending(St);
    false -> ensure_delayed_reconnect(St)
  end.

maybe_send_to_kafka_has_pending(St) ->
  case is_send_ahead_allowed(St) of
    true -> maybe_send_to_kafka_now(St);
    false -> St
  end.

maybe_send_to_kafka_now(#{linger_expire_timer := LTimer, replayq := Q, config := #{max_linger_ms := Max}} = St) ->
  First = replayq:peek(Q),
  LingerTimeout = Max - (now_ts() - get_item_ts(First)),
  case LingerTimeout =< 0 of
    true -> send_to_kafka(St);
    false when is_reference(LTimer) -> St;
    false ->
      Ref = erlang:send_after(LingerTimeout, self(), linger_expire),
      St#{linger_expire_timer := Ref}
  end.

send_to_kafka(#{sent_reqs := Sent, sent_reqs_count := SentCount, replayq := Q,
  config := #{max_batch_bytes := BytesLimit, required_acks := RequiredAcks, ack_timeout := AckTimeout, compression := Compression},
  conn := Conn, produce_api_vsn := Vsn, topic := Topic,
  partition := Partition, linger_expire_timer := LTimer} = St0) ->
  is_reference(LTimer) andalso erlang:cancel_timer(LTimer),
  {NewQ, QAckRef, Items} = replayq:pop(Q, #{bytes_limit => BytesLimit, count_limit => 999999999}),
  {FlatBatch, Calls} = get_flat_batch(Items, [], []),
  [_ | _] = FlatBatch,
  Req = kpro_req_lib:produce(Vsn, Topic, Partition, FlatBatch, #{ack_timeout => AckTimeout, required_acks => RequiredAcks, compression => Compression}),
  St1 = St0#{replayq := NewQ, linger_expire_timer := false},
  NewSent = {Req, QAckRef, Calls},
  St2 = St1#{sent_reqs := queue:in(NewSent, Sent), sent_reqs_count := SentCount + 1},
  ok = request_async(Conn, Req),
  ok = send_stats(St2, FlatBatch),
  St3 = maybe_fake_kafka_ack(Req, St2),
  maybe_send_to_kafka(St3).

maybe_fake_kafka_ack(#kpro_req{no_ack = true, ref = Ref}, St) ->
  do_handle_kafka_ack(Ref, -1, St);
maybe_fake_kafka_ack(_Req, St) -> St.

is_send_ahead_allowed(#{config := #{max_send_ahead := Max}, sent_reqs_count := SentCount}) ->
  SentCount =< Max.

is_idle(#{replayq := Q, sent_reqs_count := SentReqsCount}) ->
  SentReqsCount =:= 0 andalso replayq:count(Q) =:= 0.

now_ts() -> erlang:system_time(millisecond).

make_queue_item(CallId, Batch) ->
  {CallId, now_ts(), Batch}.

queue_item_sizer({_CallId, _Ts, Batch}) ->
  batch_bytes(Batch).

batch_bytes(Batch) ->
  lists:foldl(fun (M, Sum) -> oct(M) + Sum end, 0, Batch).

queue_item_marshaller({_, _, _} = I) ->
  term_to_binary(I);
queue_item_marshaller(Bin) when is_binary(Bin) ->
  binary_to_term(Bin).

get_item_ts({_, Ts, _}) -> Ts.

get_produce_version(#{conn := Conn} = St) when is_pid(Conn) ->
  Vsn = case kpro:get_api_vsn_range(Conn, produce) of
          {ok, {_Min, Max}} -> Max;
          {error, _} -> 3
        end,
  St#{produce_api_vsn => Vsn}.

get_flat_batch([], Msgs, Calls) ->
  {lists:reverse(Msgs), lists:reverse(Calls)};
get_flat_batch([QItem | Rest], Msgs, Calls) ->
  {CallId, _Ts, Batch} = QItem,
  get_flat_batch(Rest, lists:reverse(Batch, Msgs), [{CallId, length(Batch)} | Calls]).

handle_kafka_ack(#kpro_rsp{api = produce, ref = Ref, msg = Rsp}, St) ->
  [TopicRsp] = kpro:find(responses, Rsp),
  [PartitionRsp] = kpro:find(partition_responses, TopicRsp),
  ErrorCode = kpro:find(error_code, PartitionRsp),
  BaseOffset = kpro:find(base_offset, PartitionRsp),
  case ErrorCode =:= no_error of
    true -> do_handle_kafka_ack(Ref, BaseOffset, St);
    false ->
      #{topic := Topic, partition := Partition} = St,
      log_error("~s-~p: Produce response error-code = ~p", [Topic, Partition, ErrorCode]),
      erlang:throw(ErrorCode)
  end.

do_handle_kafka_ack(Ref, BaseOffset,#{sent_reqs := SentReqs, sent_reqs_count := SentReqsCount, pending_acks := PendingAcks, replayq := Q} = St) ->
  case queue:peek(SentReqs) of
    {value, {#kpro_req{ref = Ref}, Q_AckRef, Calls}} ->
      ok = replayq:ack(Q, Q_AckRef),
      {{value, _}, NewSentReqs} = queue:out(SentReqs),
      NewPendingAcks = evaluate_pending_ack_funs(PendingAcks, Calls, BaseOffset),
      St#{sent_reqs := NewSentReqs, sent_reqs_count := SentReqsCount - 1, pending_acks := NewPendingAcks};
    _ -> St
  end.

mark_connection_down(#{topic := Topic, partition := Partition, conn := Old} = St0, Reason) ->
  St = St0#{conn := Reason},
  case is_idle(St) of
    true -> St;
    false ->
      maybe_log_connection_down(Topic, Partition, Old, Reason),
      ensure_delayed_reconnect(St)
  end.

maybe_log_connection_down(_Topic, _Partition, _, to_be_discovered) ->
  ok;
maybe_log_connection_down(Topic, Partition, Conn, Reason) when is_pid(Conn) ->
  log_error("Producer ~s-~p: Connection ~p down. Reason: ~p", [Topic, Partition, Conn, Reason]);
maybe_log_connection_down(Topic, Partition, _, Reason) ->
  log_error("Producer ~s-~p: Failed to reconnect.Reason: ~p",[Topic, Partition, Reason]).

ensure_delayed_reconnect(#{config := #{reconnect_delay_ms := Delay0}, client_id := ClientId, topic := Topic, partition := Partition, reconnect_timer := no_timer} = St) ->
  Delay = Delay0 + rand:uniform(1000),
  case wolff_client_sup:find_client(ClientId) of
    {ok, ClientPid} ->
      Args = [ClientPid, Topic, Partition, self()],
      log_info(Topic, Partition, "Will try to rediscover leader connection after ~p ms delay", [Delay]),
      {ok, Tref} = timer:apply_after(Delay, wolff_client, recv_leader_connection, Args),
      St#{reconnect_timer => Tref};
    {error, _Restarting} ->
      log_info(Topic, Partition, "Will try to rediscover client pid after ~p ms delay", [Delay]),
      {ok, Tref} = timer:apply_after(Delay, erlang, send, [self(), reconnect]),
      St#{reconnect_timer => Tref}
  end;
ensure_delayed_reconnect(St) -> St.

evaluate_pending_ack_funs(PendingAcks, [],
    _BaseOffset) ->
  PendingAcks;
evaluate_pending_ack_funs(PendingAcks, [{CallId, BatchSize} | Rest], BaseOffset) ->
  NewPendingAcks = case maps:get(CallId, PendingAcks, false) of
                     false -> PendingAcks;
                     AckCb ->
                       ok = eval_ack_cb(AckCb, BaseOffset),
                       maps:without([CallId], PendingAcks)
                   end,
  evaluate_pending_ack_funs(NewPendingAcks, Rest, offset(BaseOffset, BatchSize)).

offset(BaseOffset, Delta) when is_integer(BaseOffset) andalso BaseOffset >= 0 ->
  BaseOffset + Delta;
offset(BaseOffset, _Delta) -> BaseOffset.

log_info(Topic, Partition, Fmt, Args) ->
  error_logger:info_msg("~s-~p: " ++ Fmt, [Topic, Partition | Args]).

log_warn(Topic, Partition, Fmt, Args) ->
  error_logger:warning_msg("~s-~p: " ++ Fmt, [Topic, Partition | Args]).

log_error(Fmt, Args) ->
  error_logger:error_msg(Fmt, Args).

send_stats(#{client_id := ClientId, topic := Topic, partition := Partition}, Batch) ->
  {Cnt, Oct} = lists:foldl(fun (Msg, {C, O}) -> {C + 1, O + oct(Msg)} end, {0, 0}, Batch),
  ok = wolff_stats:sent(ClientId, Topic, Partition, #{cnt => Cnt, oct => Oct}).

oct(#{key := K, value := V} = Msg) ->
  HeadersBytes = headers_bytes(maps:get(headers, Msg, [])),
  FieldsBytes = HeadersBytes + 7 + encoded_bin_bytes(K) + encoded_bin_bytes(V),
  FieldsBytes + varint_bytes(HeadersBytes + FieldsBytes).

headers_bytes(Headers) -> headers_bytes(Headers, 0, 0).

headers_bytes([], Bytes, Count) ->
  Bytes + varint_bytes(Count);
headers_bytes([{Name, Value} | T], Bytes, Count) ->
  NewBytes = Bytes + encoded_bin_bytes(Name) + encoded_bin_bytes(Value),
  headers_bytes(T, NewBytes, Count + 1).

-compile({inline, {encoded_bin_bytes, 1}}).
encoded_bin_bytes(B) -> varint_bytes(size(B)) + size(B).

-compile({inline, {vb, 1}}).
vb(N) when N < 128 -> 1;
vb(N) when N < 16384 -> 2;
vb(N) when N < 2097152 -> 3;
vb(N) when N < 268435456 -> 4;
vb(N) when N < 34359738368 -> 5;
vb(N) when N < 4398046511104 -> 6;
vb(N) when N < 562949953421312 -> 7;
vb(N) when N < 72057594037927936 -> 8;
vb(N) when N < 9223372036854775808 -> 9;
vb(_) -> 10.

-compile({inline, {varint_bytes, 1}}).
varint_bytes(N) -> vb(zz(N)).

-compile({inline, {zz, 1}}).
zz(I) -> I bsl 1 bxor (I bsr 63).

request_async(Conn, Req) when is_pid(Conn) ->
  ok = kpro:send(Conn, Req).

collect_send_calls(Calls, Count, Size, Limit) when Size >= Limit ->
  {lists:reverse(Calls), Count, Size};
collect_send_calls(Calls, Count, Size, Limit) ->
  receive
    {send, _, Batch, _} = Call ->
      collect_send_calls([Call | Calls],
        Count + 1,
        Size + batch_bytes(Batch),
        Limit)
  after 0 -> {lists:reverse(Calls), Count, Size}
  end.

enqueue_calls(Calls, #{replayq := Q, pending_acks := PendingAcks0, call_id_base := CallIdBase, partition := Partition} = St0) ->
  {QueueItems, PendingAcks} = lists:foldl(
    fun ({send, _From, Batch, AckFun}, {Items, PendingAcksIn}) ->
        CallId = make_call_id(CallIdBase),
        PendingAcksOut = PendingAcksIn#{CallId => {AckFun, Partition}},
        NewItems = [make_queue_item(CallId, Batch) | Items],
        {NewItems, PendingAcksOut}
    end,{[], PendingAcks0},
    Calls),
  NewQ = replayq:append(Q, lists:reverse(QueueItems)),
  lists:foreach(fun maybe_reply_queued/1, Calls),
  handle_overflow(St0#{replayq := NewQ, pending_acks := PendingAcks}, replayq:overflow(NewQ)).

maybe_reply_queued({send, no_queued_reply, _, _}) -> ok;
maybe_reply_queued({send, {Pid, Ref}, _, _}) ->
  erlang:send(Pid, {Ref, queued}).

eval_ack_cb({AckFun, Partition}, BaseOffset)
  when is_function(AckFun, 2) ->
  ok = AckFun(Partition, BaseOffset);
eval_ack_cb({{F, A}, Partition}, BaseOffset) ->
  true = is_function(F, length(A) + 2),
  ok = erlang:apply(F, [Partition, BaseOffset | A]).

handle_overflow(St, Overflow) when Overflow =< 0 ->
  ok = maybe_log_discard(St, 0),
  St;
handle_overflow(#{replayq := Q,
  pending_acks := PendingAcks} =
  St,
    Overflow) ->
  {NewQ, QAckRef, Items} = replayq:pop(Q,
    #{bytes_limit => Overflow,
      count_limit => 999999999}),
  ok = replayq:ack(NewQ, QAckRef),
  {FlatBatch, Calls} = get_flat_batch(Items, [], []),
  [_ | _] = FlatBatch,
  ok = maybe_log_discard(St, length(Calls)),
  NewPendingAcks = evaluate_pending_ack_funs(PendingAcks,
    Calls,
    buffer_overflow_discarded),
  St#{replayq := NewQ, pending_acks := NewPendingAcks}.

maybe_log_discard(St, Increment) ->
  Last = get_overflow_log_state(),
  #{last_cnt := LastCnt, acc_cnt := AccCnt} = Last,
  case LastCnt =:= AccCnt andalso Increment =:= 0 of
    true -> ok;
    false -> maybe_log_discard(St, Increment, Last)
  end.

maybe_log_discard(#{topic := Topic,
  partition := Partition},
    Increment,
    #{last_ts := LastTs, last_cnt := LastCnt,
      acc_cnt := AccCnt}) ->
  NowTs = erlang:system_time(millisecond),
  NewAccCnt = AccCnt + Increment,
  DiffCnt = NewAccCnt - LastCnt,
  case NowTs - LastTs > 5000 of
    true ->
      log_warn(Topic,
        Partition,
        "replayq_overflow_dropped_number_of_requests ~p",
        [DiffCnt]),
      put_overflow_log_state(NowTs, NewAccCnt, NewAccCnt);
    false ->
      put_overflow_log_state(LastTs, LastCnt, NewAccCnt)
  end.

get_overflow_log_state() ->
  case get(buffer_overflow_discarded) of
    undefined ->
      #{last_ts => 0, last_cnt => 0, acc_cnt => 0};
    V when is_map(V) -> V
  end.

put_overflow_log_state(Ts, Cnt, Acc) ->
  put(buffer_overflow_discarded,
    #{last_ts => Ts, last_cnt => Cnt, acc_cnt => Acc}),
  ok.

