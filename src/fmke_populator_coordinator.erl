-module(fmke_populator_coordinator).

-include("fmke_populator.hrl").

-behaviour(gen_statem).

-export([
    begin_population/0
    ,start/3
    ,ops_count/0
]).

%% gen_statem exports
-export([
    callback_mode/0
    ,init/1
    ,terminate/3
]).

%% existing states
-export([
    ready/3
    ,populate_patients/3
    ,populate_facilities/3
    ,populate_pharmacies/3
    ,populate_staff/3
    ,populate_prescriptions/3
    ,done/3
]).

-record(coordinator_state, {
    master :: pid()
    ,missing_entities = 0:: non_neg_integer()
    ,nodes = [] :: list(node())
    ,ops_done = 0 :: non_neg_integer()
    ,records_added = 0 :: non_neg_integer()
    ,workers = [] :: list(pid())
}).

-define(SERVER, ?MODULE).

callback_mode() ->
    [state_functions, state_enter].

start(Notify, NumWorkers, Nodes) ->
    gen_statem:start_link({local, ?SERVER}, ?MODULE, [Notify, NumWorkers, Nodes], []).

ops_count() ->
    gen_statem:call(?SERVER, get_ops_count).

begin_population() ->
    gen_statem:call(?SERVER, start).

init([Notify, NumWorkers, Nodes]) ->
    Workers = start_workers(NumWorkers, Nodes),
    {ok, ready,
        #coordinator_state{
            master = Notify
            ,nodes = Nodes
            ,workers = Workers
        }}.

%% Handle events common to all states, in this case only get_ops_count
handle_common({call, From}, get_ops_count, Data) ->
    {keep_state, Data, [{reply, From, Data#coordinator_state.ops_done}]}.

terminate(Reason, State, #coordinator_state{master = Master}) ->
    io:format("Coordinator crashing at state ~p with reason ~p~n", [State, Reason]),
    Master ! {error, {coord_exit, Reason}}.

ready(enter, _OldState, Data) ->
    {keep_state, Data};
ready({call, From}, start, Data) ->
    [{only_prescriptions, OnlyPrescriptions}] = ets:lookup(?ETS_TABLE, only_prescriptions),
    Reply = {reply, From, ok},
    NextState =
        case OnlyPrescriptions of
            true ->
                populate_prescriptions;
            false ->
                populate_patients
        end,
    {next_state, NextState, Data, [Reply]};
ready(EventType, EventContent, Data) ->
    handle_common(EventType, EventContent, Data).

populate_patients(enter, _State, Data) ->
    [{patients_to_add, NumRecords}] = ets:lookup(?ETS_TABLE, patients_to_add),
    NewData = Data#coordinator_state{missing_entities = NumRecords},
    ok = distribute_work(patient, NewData),
    {keep_state, NewData};
populate_patients(_Type, {done, patient, _Id}, Data) ->
    handle_done(patient, populate_facilities, Data);
populate_patients(_Type, {skipped, patient, _Id}, Data) ->
    handle_skipped(patient, populate_facilities, Data);
populate_patients(EventType, EventContent, Data) ->
    handle_common(EventType, EventContent, Data).


populate_facilities(enter, populate_patients, Data) ->
    [{facilities_to_add, NumRecords}] = ets:lookup(?ETS_TABLE, facilities_to_add),
    NewData = Data#coordinator_state{missing_entities = NumRecords},
    ok = distribute_work(facility, NewData),
    {keep_state, NewData};
populate_facilities(_Type, {done, facility, _Id}, Data) ->
    handle_done(facility, populate_pharmacies, Data);
populate_facilities(_Type, {skipped, facility, _Id}, Data) ->
    handle_skipped(facility, populate_pharmacies, Data);
populate_facilities(EventType, EventContent, Data) ->
    handle_common(EventType, EventContent, Data).

populate_pharmacies(enter, populate_facilities, Data) ->
    [{pharmacies_to_add, NumRecords}] = ets:lookup(?ETS_TABLE, pharmacies_to_add),
    NewData = Data#coordinator_state{missing_entities = NumRecords},
    ok = distribute_work(pharmacy, NewData),
    {keep_state, NewData};
populate_pharmacies(_Type, {done, pharmacy, _Id}, Data) ->
    handle_done(pharmacy, populate_staff, Data);
populate_pharmacies(_Type, {skipped, pharmacy, _Id}, Data) ->
    handle_skipped(pharmacy, populate_staff, Data);
populate_pharmacies(EventType, EventContent, Data) ->
    handle_common(EventType, EventContent, Data).

populate_staff(enter, populate_pharmacies, Data) ->
    [{staff_to_add, NumRecords}] = ets:lookup(?ETS_TABLE, staff_to_add),
    NewData = Data#coordinator_state{missing_entities = NumRecords},
    ok = distribute_work(staff, NewData),
    {keep_state, NewData};
populate_staff(_Type, {done, staff, _Id}, Data) ->
    %% if the skip_prescriptions, population is complete
    NextState = next_state_after_staff(),
    handle_done(staff, NextState, Data);
populate_staff(_Type, {skipped, staff, _Id}, Data) ->
    %% if the skip_prescriptions, population is complete
    NextState = next_state_after_staff(),
    handle_skipped(staff, NextState, Data);
populate_staff(EventType, EventContent, Data) ->
    handle_common(EventType, EventContent, Data).

%% can enter from populate_staff or from ready
populate_prescriptions(enter, _OldState, Data) ->
    [{prescriptions_to_add, NumRecords}] = ets:lookup(?ETS_TABLE, prescriptions_to_add),
    NewData = Data#coordinator_state{missing_entities = NumRecords},
    ok = distribute_work(prescription, NewData),
    {keep_state, NewData};
populate_prescriptions(_Type, {done, prescription, _Id}, Data) ->
    handle_done(prescription, done, Data);
populate_prescriptions(_Type, {skipped, prescription, _Id}, Data) ->
    handle_skipped(prescription, done, Data);
populate_prescriptions(EventType, EventContent, Data) ->
    handle_common(EventType, EventContent, Data).

done(enter, _OldState, #coordinator_state{master = Master, records_added = NumRecords} = Data) ->
    Master ! {ok, NumRecords},
    {keep_state, Data}.

next_state_after_staff() ->
    [{skip_prescriptions, SkipPrescriptions}] = ets:lookup(?ETS_TABLE, skip_prescriptions),
    case SkipPrescriptions of
        true  -> done;
        false -> populate_prescriptions
    end.

handle_skipped(Entity, NextState, #coordinator_state{missing_entities = Missing, ops_done = OpsDone} = Data) ->
    NewData = Data#coordinator_state{missing_entities = Missing - 1, ops_done = OpsDone + 1},
    case NewData#coordinator_state.missing_entities of
        0 ->
            print_entity_pop_end(Entity),
            {next_state, NextState, NewData};
        _ ->
            {keep_state, NewData}
    end.

handle_done(Entity, NextState, #coordinator_state{missing_entities = Missing, ops_done = OpsDone,
                              records_added = NumRecords} = Data) ->
    NewData = Data#coordinator_state{
        missing_entities = Missing - 1,
        ops_done = OpsDone + 1,
        records_added = NumRecords + 1
    },
    case NewData#coordinator_state.missing_entities of
        0 ->
            print_entity_pop_end(Entity),
            {next_state, NextState, NewData};
        _ ->
            {keep_state, NewData}
    end.

distribute_work(Entity, Data) ->
    NumRecords = Data#coordinator_state.missing_entities,
    Workers = Data#coordinator_state.workers,
    %% in some datasets for some entity types, it's possible that more than
    %% the required number of workers have been created. In that case, use
    %% just enough workers to equal 1 operation per worker.
    UsableWorkers = min(NumRecords, length(Workers)),
    Slices = slice(NumRecords, UsableWorkers),
    ok = print_entity_pop_begin(Entity),
    ok = assign(Entity, Slices, lists:sublist(Workers, 1, UsableWorkers)),
    ok.

print_entity_pop_end(facility) ->
    io:format("===> All hospitals added.~n");
print_entity_pop_end(patient) ->
    io:format("===> All patients added.~n");
print_entity_pop_end(pharmacy) ->
    io:format("===> All pharmacies added.~n");
print_entity_pop_end(prescription) ->
    io:format("===> All prescriptions added.~n");
print_entity_pop_end(staff) ->
    io:format("===> All doctors added.~n").

print_entity_pop_begin(facility) ->
    io:format("===> Populating hospitals...~n");
print_entity_pop_begin(patient) ->
    io:format("===> Populating patients...~n");
print_entity_pop_begin(pharmacy) ->
    io:format("===> Populating pharmacies...~n");
print_entity_pop_begin(prescription) ->
    io:format("===> Populating prescriptions...~n");
print_entity_pop_begin(staff) ->
    io:format("===> Populating doctors...~n").

-spec assign(Entity :: entity(), Slices :: list(interval()), Workers :: list(pid())) -> ok.
assign(_Entity, [], []) ->
    ok;
assign(Entity, [S1|Ss], [W1|Ws]) ->
    {registered_name, Name} = process_info(W1, registered_name),
    fmke_populator_worker:create(Name, Entity, S1),
    assign(Entity, Ss, Ws).

%% Takes a number and creates a list of intervals {From, To} that ranges from 1..N
%% e.g. slice(10, 2) = [{1,5}, {6,10}]
%% e.g. slice(300, 3) = [{1, 100}, {101, 200}, {201, 300}]
%% e.g. slice(2000000, 2) = [{1, 1000000}, {1000001, 2000000}]
-spec slice(Number :: non_neg_integer(), Shares :: non_neg_integer()) -> list(interval()).
slice(Number, Shares) when Number >= Shares ->
    SliceSize = Number div Shares,
    slice(Shares, Number, SliceSize).

slice(NumProcs, Amount, SliceSize) ->
    slice(NumProcs, NumProcs, Amount, SliceSize, []).

slice(0, _NumProcs, _Amount, _Size, Accum) ->
    Accum;
slice(ProcId, NumProcs, Amount, SliceSize, Accum) ->
    Start = (ProcId-1) * SliceSize + 1,
    End = case ProcId =:= NumProcs of
        true -> Amount;
        false -> Start + SliceSize - 1
    end,
    slice(ProcId-1, NumProcs, Amount, SliceSize, [{Start, End} | Accum]).

-spec start_workers(NumWorkers :: non_neg_integer(), Nodes :: list(node())) -> list(pid()).
start_workers(NumWorkers, Nodes) ->
    start_workers(NumWorkers, Nodes, length(Nodes), []).

start_workers(0, _Nodes, _Len, Accum) ->
    Accum;
start_workers(N, Nodes, Len, Accum) ->
    Name = list_to_atom(io_lib:format("worker_~B", [N])),
    %% use round-robin strategy to distribute workers across FMKe nodes
    Nth = (N rem Len) + 1,
    FmkeNode = lists:nth(Nth, Nodes),
    {ok, Pid} = fmke_populator_worker:start(Name, FmkeNode),
    start_workers(N-1, Nodes, Len, [Pid | Accum]).
