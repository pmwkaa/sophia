
Transaction Manager
--------------------

| name | type | description  |
|---|---|---|
| transaction.online\_rw | int, ro | Number of active RW transactions. |
| transaction.online\_ro | int, ro | Number of active RO transactions. |
| transaction.commit | int, ro | Total number of completed transactions. |
| transaction.rollback | int, ro | Total number of transaction rollbacks. |
| transaction.conflict | int, ro | Total number of transaction conflicts. |
| transaction.lock | int, ro | Total number of transaction locks. |
| transaction.latency | string, ro | Average transaction latency from begin till commit. |
| transaction.log | string, ro | Average transaction log length. |
| transaction.vlsn | int, ro | Current VLSN. |
| transaction.gc | int, ro | SSI GC queue size. |
