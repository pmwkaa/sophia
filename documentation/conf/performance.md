
Performance
-----------

| name | type | description  |
|---|---|---|
| performance.documents | int, ro | Number of currently allocated document.  |
| performance.documents\_used | int, ro | RAM used by allocated document. |
| performance.key | string, ro | Average key size. |
| performance.value | string, ro | Average value size. |
| performance.set | int, ro | Total number of Set operations. |
| performance.set\_latency | string, ro | Average Set latency. |
| performance.delete | int, ro | Total number of Delete operations. |
| performance.delete\_latency | string, ro | Average Delete latency. |
| performance.upsert | int, ro | Total number of Upsert operations. |
| performance.upsert\_latency | string, ro | Average Upsert latency. |
| performance.get | int, ro | Total number of Get operations. |
| performance.get\_latency | string, ro | Average Get latency. |
| performance.get\_read\_disk | string, ro | Average disk reads by Get operation. |
| performance.get\_read\_cache | string, ro | Average cache reads by Get operation. |
| performance.tx\_active\_rw | int, ro | Number of active RW transactions. |
| performance.tx\_active\_ro | int, ro | Number of active RO transactions. |
| performance.tx | int, ro | Total number of completed transactions. |
| performance.tx\_rollback | int, ro | Total number of transaction rollbacks. |
| performance.tx\_conflict | int, ro | Total number of transaction conflicts. |
| performance.tx\_lock | int, ro | Total number of transaction locks. |
| performance.tx\_latency | string, ro | Average transaction latency from begin till commit. |
| performance.tx\_ops | string, ro | Average number of statements per transaction. |
| performance.tx\_gc\_queue | int, ro | Transaction manager GC queue size. |
| performance.cursor | int, ro | Total number of Cursor operations. |
| performance.cursor\_latency | string, ro | Average Cursor latency. |
| performance.cursor\_read\_disk | string, ro | Average disk reads by Cursor operation. |
| performance.cursor\_read\_cache | string, ro | Average cache reads by Cursor operation. |
| performance.cursor\_ops | string, ro | Average number of keys read by Cursor operation. |
| performance.req\_queue | int, ro | Number of waiting request in async queue. |
| performance.req\_ready | int, ro | Number of ready request in async queue. |
| performance.req\_active | int, ro | Number of active request in async queue. |
| performance.reqs | int, ro | Current number of request in async queue. |
