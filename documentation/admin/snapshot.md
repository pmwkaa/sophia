
Snapshot
--------

During recovery, Sophia tries to read disk indexes. To reduce recovery time for big databases, Sophia periodically writes 
index dump to the disk generating a single *snapshot* file. This operation is called **Snapshot**.

Only indexes are saved during the operation, but database records remain untouched.

Snapshot period interval can be set or disabled using
**db.name.compaction.snapshot_period** variable.

```C
/* take snapshot every 10 minutes */
sp_setint(env, "db.test.compaction.snapshot_period", 600);
```
