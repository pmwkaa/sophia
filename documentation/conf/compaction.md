
Compaction
----------

| name | type | description  |
|---|---|---|
| compaction.redzone | int | To create a new redzone, write a percent value into *compaction* namespace. |
| compaction.redzone.mode | int | Set compaction mode. Mode 1: branch-less mode (strict 2-level storage), 2: checkpoint, 3: branch + compaction (default). |
| compaction.redzone.compact\_wm | int | Compaction operation starts when a number of node branches reaches this watermark. Cant be less than two. |
| compaction.redzone.compact\_mode | int | Set read-intensive or write-intensive compaction strategy mode. 0 - by number of branches, 1 - by temperature. |
| compaction.redzone.branch\_prio | int | Priority of branch operation. Priority is measured by a maximum number of executing workers. |
| compaction.redzone.branch\_wm | int | Branch operation starts when a size of in-memory key index reaches this watermark value. Measured in bytes. |
| compaction.redzone.branch\_age | int | Branch operation automatically starts when it detects that a node in-memory key index has not been updated in a branch\_age number of seconds. |
| compaction.redzone.branch\_age\_period | int | Scheduler starts scanning for aged node in-memory index every branch\_age\_period seconds. |
| compaction.redzone.branch\_age\_wm | int | This watermark value defines lower bound of in-memory index key size which is being checked during branch\_age operation. Measured in bytes. |
| compaction.redzone.anticache\_period | int | Check for anti-cache node election every anticache\_period seconds. |
| compaction.redzone.backup\_prio | int | Priority of backup operation. Priority is measured by a maximum number of executing workers. |
| compaction.redzone.gc\_wm | int | Garbage collection starts when watermark value reaches a certain percent of duplicates. When this value reaches a compaction, operation is scheduled. |
| compaction.redzone.gc\_db\_prio | int | Priority of a database async close/drop operation. |
| compaction.redzone.gc\_prio | int | Priority of gc operation. Priority is measured by a maximum number of executing workers. |
| compaction.redzone.gc\_period | int | Check for a gc every gc\_period seconds. |
| compaction.redzone.lru\_prio | int | Priority of LRU operation. Priority is measured by a maximum number of executing workers. |
| compaction.redzone.lru\_period | int | Run LRU scheduler every lru\_period seconds. |
| compaction.redzone.async | int | Asynchronous thread work mode: 1 - reserve thread, 2 - do not reserve thread. |
