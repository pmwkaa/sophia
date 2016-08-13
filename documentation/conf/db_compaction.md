
Database Compaction settings
----------------------------

| name | type | description  |
|---|---|---|
| db.name.compaction.cache | int | Total write cache size used for compaction. |
| db.name.compaction.node\_size | int | Set a node file size in bytes. Node file can grow up to two times the size before the old node file is being split. |
| db.name.compaction.page\_size | int | Set size of a page to use. |
| db.name.compaction.page\_checksum | int | Check checksum during compaction. |
| db.name.compaction.expire\_period | int | Run expire check process every expire\_period seconds. |
| db.name.compaction.gc\_wm | int | Garbage collection starts when watermark value reaches a certain percent of duplicates. When this value reaches a compaction, operation is scheduled. |
| db.name.compaction.gc\_period | int | Check for a gc every gc\_period seconds. |
