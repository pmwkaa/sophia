
Compaction
----------

Sophia scheduler is responsible for planning all background tasks.

The schedule handles following tasks: garbage collection, node compaction, expire, log ration, and so on.

Sophia has multi-thread scallable compaction. Number of active background
workers (threads) can be set using **scheduler.threads** variable.

```C
sp_setint(env, "scheduler.threads", 5);
```

Please take a look at the [Compaction](../conf/compaction.md) and [Scheduler](../conf/scheduler.md)
configuration sections for more details.
