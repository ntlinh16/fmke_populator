-define(APP, fmke_populator).
-define(ETS_TABLE, fmke_populator_tab).

-define(TIMEOUT, 10).

-define(NUM_PROCS, 100).
-define(NUM_RETRIES, 3).
-define(UPDATE_INTERVAL, 10).
-define(QUEUE_SIZE, 100).

-define(EXIT_NO_ARGS, 1).
-define(EXIT_ARGS_W_HELP, 2).
-define(EXIT_PING_FAILURE, 3).
-define(EXIT_NO_DIST, 4).
-define(EXIT_POP_ERROR, 5).
-define(EXIT_COORD_DIED, 6).
-define(EXIT_CONFLICTING_FLAGS, 7).

-define(ALL, "all").
-define(BENCH_STANDARD, "standard").
-define(NODE_NAME, 'fmke_populator@127.0.0.1').
-define(COOKIE, fmke).

-type entity() :: facility | patient | pharmacy | prescription | staff.
-type interval() :: {non_neg_integer(), non_neg_integer()}.
-type slice() :: {create, entity(), interval()} | none.
