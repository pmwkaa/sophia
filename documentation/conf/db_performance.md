
Database Performance
--------------------

| name | type | description  |
|---|---|---|
| db.name.stat.documents\_used | int, ro | Memory used by allocated document. |
| db.name.stat.documents | int, ro | Number of currently allocated document.  |
| db.name.stat.field | string, ro | Average field size. |
| db.name.stat.set | int, ro | Total number of Set operations. |
| db.name.stat.set\_latency | string, ro | Average Set latency. |
| db.name.stat.delete | int, ro | Total number of Delete operations. |
| db.name.stat.delete\_latency | string, ro | Average Delete latency. |
| db.name.stat.upsert | int, ro | Total number of Upsert operations. |
| db.name.stat.upsert\_latency | string, ro | Average Upsert latency. |
| db.name.stat.get | int, ro | Total number of Get operations. |
| db.name.stat.get\_latency | string, ro | Average Get latency. |
| db.name.stat.get\_read\_disk | string, ro | Average disk reads by Get operation. |
| db.name.stat.get\_read\_cache | string, ro | Average cache reads by Get operation. |
| db.name.stat.pread | int, ro | Total number of pread operations. |
| db.name.stat.pread\_latency | string, ro | Average pread latency. |
| db.name.stat.cursor | int, ro | Total number of Cursor operations. |
| db.name.stat.cursor\_latency | string, ro | Average Cursor latency. |
| db.name.stat.cursor\_read\_disk | string, ro | Average disk reads by Cursor operation. |
| db.name.stat.cursor\_read\_cache | string, ro | Average cache reads by Cursor operation. |
| db.name.stat.cursor\_ops | string, ro | Average number of keys read by Cursor operation. |
