-module(fmke_dataset).

-include("fmke_dataset_minimal.hrl").
-include("fmke_dataset_small.hrl").
-include("fmke_dataset_medium.hrl").
-include("fmke_dataset_standard.hrl").

-export([
    get_entities/1
    ,get_zipf_params/1
    ,num_facilities/1
    ,num_patients/1
    ,num_pharmacies/1
    ,num_prescriptions/1
    ,num_staff/1
]).

get_entities(Dataset) ->
    #{
        facilities => num_facilities(Dataset)
        ,patients => num_patients(Dataset)
        ,pharmacies => num_pharmacies(Dataset)
        ,prescriptions => num_prescriptions(Dataset)
        ,staff => num_staff(Dataset)
    }.

get_zipf_params("minimal") ->
    #{
        size => ?MIN_ZIPF_SIZE
        ,skew => ?MIN_ZIPF_SKEW
    };
get_zipf_params("small") ->
    #{
        size => ?SMALL_ZIPF_SIZE
        ,skew => ?SMALL_ZIPF_SKEW
    };
get_zipf_params("medium") ->
    #{
        size => ?MEDIUM_ZIPF_SIZE
        ,skew => ?MEDIUM_ZIPF_SKEW
    };
get_zipf_params("standard") ->
    #{
        size => ?STD_ZIPF_SIZE
        ,skew => ?STD_ZIPF_SKEW
    }.

num_facilities("minimal") ->
    ?MIN_NUM_FACILITIES;
num_facilities("small") ->
    ?SMALL_NUM_FACILITIES;
num_facilities("medium") ->
    ?MEDIUM_NUM_FACILITIES;
num_facilities("standard") ->
    ?STD_NUM_FACILITIES.

num_patients("minimal") ->
    ?MIN_NUM_PATIENTS;
num_patients("small") ->
    ?SMALL_NUM_PATIENTS;
num_patients("medium") ->
    ?MEDIUM_NUM_PATIENTS;
num_patients("standard") ->
    ?STD_NUM_PATIENTS.

num_pharmacies("minimal") ->
    ?MIN_NUM_PHARMACIES;
num_pharmacies("small") ->
    ?SMALL_NUM_PHARMACIES;
num_pharmacies("medium") ->
    ?MEDIUM_NUM_PHARMACIES;
num_pharmacies("standard") ->
    ?STD_NUM_PHARMACIES.

num_prescriptions("minimal") ->
    ?MIN_NUM_PRESCRIPTIONS;
num_prescriptions("small") ->
    ?SMALL_NUM_PRESCRIPTIONS;
num_prescriptions("medium") ->
    ?MEDIUM_NUM_PRESCRIPTIONS;
num_prescriptions("standard") ->
    ?STD_NUM_PRESCRIPTIONS.

num_staff("minimal") ->
    ?MIN_NUM_STAFF;
num_staff("small") ->
    ?SMALL_NUM_STAFF;
num_staff("medium") ->
    ?MEDIUM_NUM_STAFF;
num_staff("standard") ->
    ?STD_NUM_STAFF.
