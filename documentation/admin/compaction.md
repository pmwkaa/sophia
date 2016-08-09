
Compaction
----------

Sophia Scheduler is responsible for planning all background tasks.

The schedule handles following tasks: garbage collection, branch compaction,
node compaction, log ration, and so on.

Sophia has multi-thread scallable compaction. Number of active background
workers (threads) can be set using **scheduler.threads** variable.

```C
sp_setint(env, "scheduler.threads", 5);
```

Please take a look at the [Compaction](../conf/compaction.md) and [Scheduler](../conf/scheduler.md)
configuration sections for more details.

Checkpoint
----------

It is possible to start incremental asynchronous checkpointing process, which will
force branch creation and memory freeing for every node in-memory index.
Once a memory index log is free, files also will be automatically garbage-collected.

```C
sp_setint(env, "db.test.checkpoint", 0);
```

Procedure call is fast and does not block. **db.name.scheduler.checkpoint\_active** and
**db.name.scheduler.checkpoint\_lsn\_last** variables can be examined to see if checkpoint
process is completed.

Checkpoints are automatically used to ensure a memory limit.
