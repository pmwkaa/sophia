
Snapshot
--------

During recovery, Sophia tries to read disk indexes. To reduce recovery time for big databases, Sophia periodically writes 
index dump to the disk generating a single *snapshot* file. This operation is called **Snapshot**.

Only indexes are saved during the operation, but database records remain untouched.

Snapshot period interval can be set or disabled using
**compaction.zone.snapshot_period** variable.

```C
/* take snapshot every 10 minutes */
sp_setint(env, "compaction.0.snapshot_period", 360);
```

Another important purpose of Snapshot is saving necessary
statistic to distinct Hot and Cold node files used by
[Anti-Cache](anticache.md) storage mode.
