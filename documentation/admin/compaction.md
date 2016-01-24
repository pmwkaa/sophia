
Compaction
----------

Sophia Scheduler is responsible for planning all background tasks depending on
current system load and selected profile (redzone).

The schedule handles following tasks: garbage collection, branch compaction,
node compaction, log ration, lru, anti-cache election, async reads, and so on.

Sophia has multi-thread scallable compaction. Number of active background
workers (threads) can be set using **scheduler.threads** variable.

```C
sp_setint(env, "scheduler.threads", 5);
```

Please take a look at the [Compaction](../conf/compaction.md) and [Scheduler](../conf/scheduler.md)
configuration sections for more details.

Compaction Configuration
------------------------

Sophia compaction process is configurable via **redzone**. Redzone
is a special value which represents current memory usage. Each redzone
defines the background operations' priority, etc.

If no memory limit is set, redzone zero is used (default).

To create a new redzone, write a percent value into **compaction** namespace.

By default only *compaction.0* and *compaction.80* redzones are defined. When 80 percent
of the memory usage is reached, checkpoint process starts automatically.

Checkpoint
----------

It is possible to start incremental asynchronous checkpointing process, which will
force branch creation and memory freeing for every node in-memory index.
Once a memory index log is free, files also will be automatically garbage-collected.

```C
sp_setint(env, "scheduler.checkpoint", 0);
```

Procedure call is fast and does not block. **scheduler.checkpoint\_active** and
**scheduler.checkpoint\_lsn\_last** variables can be examined to see if checkpoint
process is completed.

Checkpoints are automatically used to ensure a memory limit.
