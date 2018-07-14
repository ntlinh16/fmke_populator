-module(fmke_populator_perf_monitor).

-behaviour(gen_server).

-export([start/1, stop/0]).

%% gen_server exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(SERVER, ?MODULE).

start(Update) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Update], []).

stop() ->
    gen_server:stop(?SERVER).

init([Update]) ->
    erlang:send_after(Update*1000, self(), update),
    {ok, {Update, 0}}.

handle_call(_From, _Msg, State) ->
    {noreply, State}.

handle_cast(_Msg, Update) ->
    {noreply, Update}.

handle_info(update, {Update, NumOps}) ->
    Ops = fmke_populator_coordinator:ops_count(),
    Delta = Ops-NumOps, %% Ops >= NumOps is always true
    io:format("Current average throughput ~p ops/s, ~p total records added.~n", [trunc(Delta/Update), Ops]),
    erlang:send_after(Update*1000, self(), update),
    {noreply, {Update, Ops}}.
