%%%-------------------------------------------------------------------
%%% @author kevin-T
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. 8ζ 2021 δΈε1:53
%%%-------------------------------------------------------------------
-module(wolff_app).
-author("kevin-T").

-behaviour(application).
-export([start/2, stop/1]).

start(_, _) ->
  wolff_sup:start_link().

stop(_) -> ok.
