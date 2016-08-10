
Database Compaction settings
----------------------------

| name | type | description  |
|---|---|---|
| db.name.compaction.mode | int | Set compaction mode. Mode 1: branch-less mode (strict 2-level storage), 2: checkpoint, 3: branch + compaction (default). |
| db.name.compaction.node\_size | int | Set a node file size in bytes. Node file can grow up to two times the size before the old node file is being split. |
| db.name.compaction.page\_size | int | Set size of a page to use. |
| db.name.compaction.page\_checksum | int | Check checksum during compaction. |
| db.name.compaction.checkpoint\_wm | int | Trigger automatic checkpoint process, when memory usage reaches checkpoint\_wm value. |
| db.name.compaction.compact\_wm | int | Compaction operation starts when a number of node branches reaches this watermark. Cannot be less than two. |
| db.name.compaction.branch\_wm | int | Branch operation starts when a size of in-memory key index reaches this watermark value. Measured in bytes. |
| db.name.compaction.expire\_period | int | Run expire check process every expire\_period seconds. |
| db.name.compaction.gc\_wm | int | Garbage collection starts when watermark value reaches a certain percent of duplicates. When this value reaches a compaction, operation is scheduled. |
| db.name.compaction.gc\_period | int | Check for a gc every gc\_period seconds. |
