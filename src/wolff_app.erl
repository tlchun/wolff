%%%-------------------------------------------------------------------
%%% @author kevin-T
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. 8月 2021 下午1:53
%%%-------------------------------------------------------------------
-module(wolff_app).
-author("kevin-T").

-behaviour(application).
-export([start/2, stop/1]).

start(_, _) ->
  wolff_sup:start_link().

stop(_) -> ok.
