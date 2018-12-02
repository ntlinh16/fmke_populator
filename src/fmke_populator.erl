-module(fmke_populator).

-include("fmke_populator.hrl").

-define(OPTSPEC, [
    {cookie, $c, "cookie", {atom, ?COOKIE}, "Cookie value to connect to FMKe servers via Distributed Erlang"},
    {dataset, $d, "dataset", {atom, ?BENCH_STANDARD}, "Data set to populate (e.g. standard | small | minimal)"},
    {force, $f, "force", {boolean, false}, "Continue even if some of the records are already present"},
    {help, $h, "help", false, "Shows this message"},
    {nodename, $n, "nodename", {atom, ?NODE_NAME}, "Erlang node name for the populator script (use longnames)"},
    {procs, $p, "processes", {integer, ?NUM_PROCS}, "Number of processes to perform the population ops"},
    {queue, $q, "queuesize", {integer, ?QUEUE_SIZE}, "Maximum aggregated queue size of all database nodes"},
    {retries, $r, "retries", {integer, ?NUM_RETRIES}, "Number of retries if operation times out"},
    {timeout, $t, "timeout", {integer, ?TIMEOUT}, "Timeout in seconds for each operation"},
    {update, $u, "update", {integer, ?UPDATE_INTERVAL}, "Seconds between periodic update and performance report"},
    {skip_prescriptions, undefined, "noprescriptions", {boolean, false}, "Skips the population of prescription records"},
    {only_prescriptions, undefined, "onlyprescriptions", {boolean, false}, "Only populates prescription records"}
]).
-define(PRINT_USAGE, getopt:usage(?OPTSPEC,
                                  "fmke_populator",
                                  "<nodes>",
                                  [{"<nodes>", "Erlang node names of servers running FMKe"}])).

%% API exports
-export([main/1]).


%%====================================================================
%% API functions
%%====================================================================

%% escript Entry point
main(CliArgs) ->
    {ok, {Opts, Args}} = getopt:parse(?OPTSPEC, CliArgs),
    maybe_print_usage(Args, Opts),
    io:format("Using opts=~p~n", [Opts]),
    %% read all relevant options from Args and Opts
    Nodes = lists:map(fun list_to_atom/1, Args),
    Dataset = proplists:get_value(dataset, Opts),
    Nodename = proplists:get_value(nodename, Opts),
    Cookie = proplists:get_value(cookie, Opts),
    Update = proplists:get_value(update, Opts),
    Timeout = proplists:get_value(timeout, Opts)*1000,
    Retries = proplists:get_value(retries, Opts),
    Continue = proplists:get_value(force, Opts),
    NumProcesses = proplists:get_value(procs, Opts),
    SkipPrescriptions = proplists:get_value(skip_prescriptions, Opts, false),
    OnlyPrescriptions = proplists:get_value(only_prescriptions, Opts, false),
    %% start distribution layer and attempt to establish contact with target nodes
    start_node(Nodename, Cookie),
    pong = try_multi_ping(Nodes),
    {ok, Driver} = rpc:call(hd(Nodes), application, get_env, [fmke, driver]),
    {ok, Database} = rpc:call(hd(Nodes), application, get_env, [fmke, target_database]),
    io:format("FMKe node is using module ~p as the driver.~n", [Driver]),
    %% check total number of records according to dataset
    TotalFacilities = fmke_dataset:num_facilities(Dataset),
    TotalPatients = fmke_dataset:num_patients(Dataset),
    TotalPharmacies = fmke_dataset:num_pharmacies(Dataset),
    TotalPrescriptions = fmke_dataset:num_prescriptions(Dataset),
    TotalDoctors = fmke_dataset:num_staff(Dataset),
    %% check number of records that are to be created according to active options
    DoctorsToPop = num_or_zero(TotalDoctors, OnlyPrescriptions),
    FacilitiesToPop = num_or_zero(TotalFacilities, OnlyPrescriptions),
    PatientsToPop = num_or_zero(TotalPatients, OnlyPrescriptions),
    PharmaciesToPop = num_or_zero(TotalPharmacies, OnlyPrescriptions),
    PrescriptionsToPop = num_or_zero(TotalPrescriptions, SkipPrescriptions),
    %% create ETS table and write some values to it
    TabId = ets:new(?ETS_TABLE, [set, protected, named_table]),
    true = ets:insert(TabId, {timeout, Timeout}),
    true = ets:insert(TabId, {retries, Retries}),
    true = ets:insert(TabId, {total_facilities, TotalFacilities}),
    true = ets:insert(TabId, {total_patients, TotalPatients}),
    true = ets:insert(TabId, {total_pharmacies, TotalPharmacies}),
    true = ets:insert(TabId, {total_prescriptions, TotalPrescriptions}),
    true = ets:insert(TabId, {total_staff, TotalDoctors}),
    true = ets:insert(TabId, {facilities_to_add, FacilitiesToPop}),
    true = ets:insert(TabId, {patients_to_add, PatientsToPop}),
    true = ets:insert(TabId, {pharmacies_to_add, PharmaciesToPop}),
    true = ets:insert(TabId, {prescriptions_to_add, PrescriptionsToPop}),
    true = ets:insert(TabId, {staff_to_add, DoctorsToPop}),
    true = ets:insert(TabId, {skip_prescriptions, SkipPrescriptions}),
    true = ets:insert(TabId, {only_prescriptions, OnlyPrescriptions}),
    true = ets:insert(TabId, {continue_if_exists, Continue}),
    true = ets:insert(TabId, {mod, fmke}),
    %% start coordinator, reporting, and zipf server process
    #{size := ZipfSize, skew := ZipfSkew} = fmke_dataset:get_zipf_params(Dataset),
    fmke_populator_zipf:start(ZipfSize, ZipfSkew),
    start_coordinator(NumProcesses, Nodes),
    maybe_start_report_server(Update),
    Start = erlang:monotonic_time(),
    %% Begin actual population
    io:format("Populating ~p...~n", [Database]),
    fmke_populator_coordinator:begin_population(),
    receive
        {error, {coord_exit, Reason}} ->
            maybe_shutdown_report_server(Update),
            io:format("Error - Population coordinator process died:~n~p~n", [Reason]),
            erlang:halt(?EXIT_COORD_DIED);
        {error, Error} ->
            maybe_shutdown_report_server(Update),
            io:format("Error - Population terminated abrubtly due to internal error:~p~n", [Error]),
            erlang:halt(?EXIT_POP_ERROR);
        {ok, NumOps} ->
            maybe_shutdown_report_server(Update),
            End = erlang:monotonic_time(),
            TimeDiff = erlang:convert_time_unit(End-Start, native, second),
            print_result(NumOps, TimeDiff),
            ok
    end.

print_result(0, Time) ->
    io:format("No entities were added, perhaps database is already populated?~n"
            "Scanned through dataset keyspace in ~p seconds~n", [Time]);
print_result(Ops, Time) when Time < 60 ->
    io:format("Populated ~p entities in ~p seconds (avg ~p ops/s).~n", [Ops, Time, trunc(Ops/Time)]);
print_result(Ops, Time) ->
    Minutes = Time div 60,
    Seconds = Time rem 60,
    io:format("Populated ~p entities in ~pm~ps (avg ~p ops/s).~n", [Ops, Minutes, Seconds, trunc(Ops/Time)]).

%%====================================================================
%% Internal functions
%%====================================================================

num_or_zero(_N, true) -> 0;
num_or_zero(N, false) -> N.

start_coordinator(NumWorkers, Nodes) ->
    {ok, _Pid} = fmke_populator_coordinator:start(self(), NumWorkers, Nodes),
    ok.

start_node(Name, Cookie) ->
    case net_kernel:start([Name, longnames]) of
        {ok, _Pid} ->
            true = erlang:set_cookie(Name, Cookie),
            io:format("Booted distributed erlang node ~p, using cookie ~p~n", [Name, Cookie]),
            ok;
        {error, Reason} ->
            io:format("Error: unable to start distributed erlang node: ~p~n", [Reason]),
            erlang:halt(?EXIT_NO_DIST)
    end.

try_multi_ping([]) ->
    pong;
try_multi_ping([H|T]) ->
    case net_adm:ping(H) of
        pong ->
            try_multi_ping(T);
        pang ->
            io:format("Error: could not ping FMKe node: ~p~n", [H]),
            erlang:halt(?EXIT_PING_FAILURE)
    end.

maybe_start_report_server(Update) when Update =< 0 ->
    io:format("Periodic reporting disabled (update flag set to ~p).~n", [Update]),
    ok;
maybe_start_report_server(Update) ->
    {ok, _Pid} =  fmke_populator_perf_monitor:start(Update),
    io:format("Booted performance report server with an update cycle of ~p seconds.~n", [Update]),
    ok.

maybe_shutdown_report_server(Update) when Update =< 0 ->
    %% report server was not started
    ok;
maybe_shutdown_report_server(_Update) ->
    fmke_populator_perf_monitor:stop().

maybe_print_usage([], _Opts) ->
    ?PRINT_USAGE,
    erlang:halt(?EXIT_NO_ARGS);
maybe_print_usage(_Args, [_H|_T] = Args) ->
    case lists:member(help, Args) of
        true ->
            ?PRINT_USAGE,
            erlang:halt(?EXIT_ARGS_W_HELP);
        false ->
            ok
    end,
    case {lists:member(skip_prescriptions, Args), lists:member(only_prescriptions, Args)} of
        {true, true} ->
            io:format("Error: --noprescriptions and --onlyprescriptions are mutually exclusive, please use only one!"),
            ?PRINT_USAGE,
            erlang:halt(?EXIT_CONFLICTING_FLAGS);
        _ ->
            ok
    end.
