# fmke_populator
![Erlang Version](https://img.shields.io/badge/Erlang%2FOTP-21-brightgreen.svg)
[![Build Status](https://travis-ci.org/goncalotomas/fmke_populator.svg?branch=master)](https://travis-ci.org/goncalotomas/fmke_populator)

A command line tool to populate databases prior to the [FMKe benchmark][3]. Requires [Erlang][1] and [rebar3][2] to build, or just Erlang if you download a release binary.

## About

The FMKe benchmark uses a dataset, and during its execution the database is assumed to be populated with that dataset. This script populates a set of entities by making thousands of RPC calls to a running FMKe server. For instance, in the standard dataset the following entities are added (in sequence):

* 1000000 patients
* 50 hospitals
* 300 pharmacies
* 10000 doctors
* 5000 prescriptions

The FMKe benchmark contains a significant amount of write operations that may impact results, so datasets are considered to be invalid for re-execution.

A configurable number workers are used to accomplish this, and a coordinator process will divide work into slices and hand them out for workers to complete. The slice size is calculated per entity type. It is possible to have more waiting workers than records to populate, in which case the coordinator will assign slices to a smaller, more appropriate number of workers.

### Execution timeline
Only a single entity type is populated at any instance. This means that hospitals will only be added once patients have all been created, and likewise for the other entity types. The population follows the following order:

patients → hospitals → pharmacies → doctors → prescriptions

Periodically the script will output basic throughput statistics, and in the end a final report will list how many records were created, the elapsed time and the average throughput and latency.

### Failing workers

Workers report the coordinator process before crashing with their current state, which will allow the coordinator to know what operations of the worker's slice are still incomplete and assign them to other workers. If more than 10% of the processes fail the coordinator will stop and exit as well.

## Building

    $ rebar3 escriptize

## Running

After running the `rebar3 escriptize` command you will get an executable binary in `_build/default/bin/`. You can use it as follows:

    $ ./fmke_populator [list_of_options] [list_of_flags] <list_of_nodes>

Where `<list_of_nodes>` is required and should be passed in as a list of Erlang node names that are running the FMKe application server, separated by spaces.

    $ ./fmke_populator fmke@192.168.1.1 fmke@192.168.1.2 fmke@192.168.1.3 fmke@192.168.1.4


## Run fmke_populator with a Docker container
#####1. Get the fmke_populator Docker image

You can build the Docker image locally
```
git clone https://github.com/ntlinh16/fmke_populator.git
cd fmke_populator/
docker build -t fmke_pop:local .
```
or get the image from the Docker repository:

```
docker pull ntlinh/fmke_pop:latest
```

Please remember provide the correct image name to run a Docker container in the Section 2.

#####2. Run a FMKe container

If you already had a FMKe app server is running, you firts need to get the FMKe's IP. Then, you pass this IP to fmke_populatore docker as follow:

```
IP=$((docker inspect fmke | grep IPAddress ) | grep -oP '(?<=: ").*(?=")');
docker run --name fmke_pop -h fmke_pop fmke_pop:local fmke@$IP
```
After these comments, docker `fmke_pop` will populate data to the database. You can check all the options in the next Section to run the population successfully.

In my experiences, I will first populate the databse with no prescription, and then populate the database with only the prescription as follow:

```
IP=$((docker inspect fmke | grep IPAddress ) | grep -oP '(?<=: ").*(?=")');
docker run --name fmke_pop1 -h fmke_pop fmke_pop:local -f --noprescriptions fmke@$IP
```

```
IP=$((docker inspect fmke | grep IPAddress ) | grep -oP '(?<=: ").*(?=")');
docker run --name fmke_pop2 -h fmke_pop fmke_pop:local -f --onlyprescriptions -p 1 fmke@$IP
```

## Options

Some configuration is allowed through the use of command line options, which override the existing defaults. Below is a table of options you can use:

| Short option | Long Option | Description |
| --- | --- | --- |
| `-c` | `--cookie` | Uses the specified value to connect to the FMKe servers via Distributed Erlang. |
| `-d` | `--dataset` | Data set to populate (use `standard`, `small` or `minimal`). |
| `-n` | `--nodename` | Node name to be used in Distributed Erlang. Must be `longnames`. |
| `-p` | `--processes` | Specifies the number of processes to use concurrently.<br>Since the script is extremely IO/bound, increasing this value will generally translate into higher throughput. |
| `-r` | `--retries` | Number of retries per single entity in case of timeout. |
| `-t` | `--timeout` | Timeout value for each operation in seconds. |
| `-u` | `--update` | Seconds between periodic update and performance report.<br>Specifying a value lesser than 1 will disable periodic reporting. |

Here is an example running the script with nodename `populator@10.10.200.1` using 100 processes without retries:

    $ ./fmke_populator -n populator@10.10.200.1 -p 100 -r 0 fmke@10.10.200.2 fmke@10.10.200.3

## Flags
Flags are different from options since they do not require parameters to be passed in. Below is the list of available flags:

| Short name | Long name | Description |
| --- | --- | --- |
| `-h` | `--help` | Lists the options and flags as well as the script usage. |
| `-f` | `--force` | Continue even if some of the records are already in the database.<br>Useful to finish a previous population that was stopped due to network connectivity issues. |
|  | `--noprescriptions` | Skips the population of prescription records. |
|  | `--onlyprescriptions` | Only populates prescription records. |

Note that only some options have a short name. Others will have to be referred to by their long name. Also, `--noprescriptions` and `--onlyprescriptions` are mutually exclusive, and including both flags will return an error.

For example, should we need to finish a script that halted halfway through populating prescriptions, we can run:

    $ ./fmke_populator -f --onlyprescriptions fmke@10.10.200.2 fmke@10.10.200.3



[1]: http://www.erlang.org/downloads
[2]: http://www.rebar3.org/
[3]: https://github.com/goncalotomas/FMKe
