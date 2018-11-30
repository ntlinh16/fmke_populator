-module(fmke_populator_worker).

-behaviour(gen_statem).

-include("fmke_populator.hrl").

%% API
-export([
    start/2
    ,create/3
]).

%% gen_statem exports
-export([
    callback_mode/0
    ,init/1
    ,format_status/2
    ,idle/3
    ,working/3
    ,terminate/3
]).

-record(worker_state, {
    interval :: interval()
    ,node :: node()
    ,slice :: slice()
}).

start(Name, Node) ->
    gen_statem:start_link({local, Name}, ?MODULE, [Node], []).

create(Name, Entity, Interval) ->
    gen_statem:call(Name, {create, Entity, Interval}).

callback_mode() ->
    state_functions.

init([Node]) ->
    process_flag(trap_exit, true),
    {ok, idle, #worker_state{node = Node}}.

format_status(_Opt, [_PDict, State, Data]) ->
    [{data, [{"State", {State, Data}}]}].

terminate(Reason, _State, #worker_state{slice = Slice}) ->
    fmke_populator_coordinator ! {worker_exit, self(), Reason, Slice}.

idle({call, From}, {create, Entity, Interval}, Data) ->
    {next_state, working, Data#worker_state{slice = {Entity, Interval}, interval = Interval},
        [{reply, From, ok}, {next_event, internal, do_work}]}.

working(_Type, do_work, #worker_state{slice = {Entity, {To, To}}} = Data) ->
    do_create(Entity, To, Data),
    {next_state, idle, Data#worker_state{slice = none, interval = none}};
working(_Type, do_work, #worker_state{slice = {Entity, {From, To}}} = Data) ->
    do_create(Entity, From, Data),
    {keep_state, Data#worker_state{slice = {Entity, {From+1, To}}}, [{next_event, internal, do_work}]}.

do_create(facility, Id, #worker_state{node = Node}) ->
    create_facility(Node, Id);
do_create(patient, Id, #worker_state{node = Node}) ->
    create_patient(Node, Id);
do_create(pharmacy, Id, #worker_state{node = Node}) ->
    create_pharmacy(Node, Id);
do_create(staff, Id, #worker_state{node = Node}) ->
    create_staff(Node, Id);
do_create(prescription, Id, #worker_state{node = Node}) ->
    create_prescription(Node, Id).

create_facility(Node, Id) ->
    Result = rpc(Node, create_facility, [Id, gen_random_name(), gen_random_address(), gen_random_type()]),
    notify_coord(facility, Id, Result),
    ok.

create_patient(Node, Id) ->
    Result = rpc(Node, create_patient, [Id, gen_random_name(), gen_random_address()]),
    notify_coord(patient, Id, Result),
    ok.

create_pharmacy(Node, Id) ->
    Result = rpc(Node, create_pharmacy, [Id, gen_random_name(), gen_random_address()]),
    notify_coord(pharmacy, Id, Result),
    ok.

create_staff(Node, Id) ->
    Result = rpc(Node, create_staff, [Id, gen_random_name(), gen_random_address(), gen_random_type()]),
    notify_coord(staff, Id, Result),
    ok.

create_prescription(Node, Id) ->
    [{total_pharmacies, NumPharmacies}] = ets:lookup(?ETS_TABLE, total_pharmacies),
    [{total_staff, NumDoctors}] = ets:lookup(?ETS_TABLE, total_staff),
    PatientId = fmke_populator_zipf:next(),
    PrescriberId = rand:uniform(NumDoctors),
    PharmacyId = rand:uniform(NumPharmacies),
    Result = rpc(Node, create_prescription, [
        Id, PatientId, PrescriberId, PharmacyId
        ,gen_random_date() %% date prescribed
        ,gen_random_drugs() %% prescription drugs
    ]),
    notify_coord(prescription, Id, Result),
    ok.

notify_coord(Entity, Id, ok) ->
    fmke_populator_coordinator ! {done, Entity, Id},
    ok;
notify_coord(Entity, Id, skipped) ->
    fmke_populator_coordinator ! {skipped, Entity, Id},
    ok.

rpc(Node, Op, Params) ->
    [{timeout, Timeout}] = ets:lookup(?ETS_TABLE, timeout),
    [{retries, Retries}] = ets:lookup(?ETS_TABLE, retries),
    [{continue_if_exists, Continue}] = ets:lookup(?ETS_TABLE, continue_if_exists),
    [{mod, Module}] = ets:lookup(?ETS_TABLE, mod),
    rpc(Node, Module, Op, Params, Timeout, 1+Retries, Continue).

rpc(Node, Module, Op, Params, Timeout, 0, _Continue) ->
    exit({op_failed, Node, {Module, Op, Params, Timeout}});
rpc(Node, Module, Op, Params, Timeout, Tries, Continue) ->
    case rpc:call(Node, Module, Op, Params, Timeout) of
        ok ->
            ok;
        {error, facility_id_taken} when Continue == true ->
            skipped;
        {error, patient_id_taken} when Continue == true ->
            skipped;
        {error, pharmacy_id_taken} when Continue == true ->
            skipped;
        {error, prescription_id_taken} when Continue == true ->
            skipped;
        {error, staff_id_taken} when Continue == true ->
            skipped;
        {badrpc, timeout} ->
            rpc(Node, Op, Params, Timeout, Tries-1, Continue);
        {badrpc, nodedown} ->
            exit({nodedown, Node});
        {badrpc, Other} ->
            exit({remote_error, Other});
        Other ->
            exit({bad_result, Other})
    end.

gen_random_drugs() ->
    NumDrugs = rand:uniform(3),
    lists:map(fun(_) -> gen_random_name() end, lists:seq(1,NumDrugs)).

gen_random_name() ->
    gen_random_string(15 + rand:uniform(25)).

gen_random_address() ->
    gen_random_string(20 + rand:uniform(20)).

gen_random_type() ->
    gen_random_string(8 + rand:uniform(10)).

gen_random_date() ->
    gen_random_string(10).

gen_random_string(NumBytes) when NumBytes > 0 ->
    binary_to_list(base64:encode(crypto:strong_rand_bytes(NumBytes))).
