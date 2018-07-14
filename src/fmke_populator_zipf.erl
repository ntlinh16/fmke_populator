-module(fmke_populator_zipf).

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-export([next/0, start/2, stop/0]).
%% gen_server exports
-export([init/1, handle_call/3, handle_cast/2]).

start(Size, Skew) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Size, Skew], []).

init([Size, Skew]) ->
    Bottom = 1 / (lists:foldl(fun(X, Sum) -> Sum + (1 / math:pow(X, Skew)) end, 0, lists:seq(1, Size))),
    {ok, {Size, Skew, Bottom}}.

next() ->
    gen_server:call(?SERVER, next).

handle_call(next, _From, {Size, Skew, Bottom}) ->
    Dice = rand:uniform(),
    {reply, next(Dice, Size, Skew, Bottom, 0, 1), {Size, Skew, Bottom}}.

next(Dice, _Size, _Skew, _Bottom, Sum, CurrRank) when Sum >= Dice -> CurrRank - 1;
next(Dice, Size, Skew, Bottom, Sum, CurrRank) ->
    NextRank = CurrRank + 1,
    Sumi = Sum + (Bottom / math:pow(CurrRank, Skew)),
    next(Dice, Size, Skew, Bottom, Sumi, NextRank).

stop() ->
    gen_server:call(?SERVER, stop).

handle_cast(_Msg, State) ->
    {noreply, State}.
