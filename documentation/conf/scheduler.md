
Scheduler
---------

| name | type | description  |
|---|---|---|
| scheduler.threads | int | Set a number of worker threads. |
| scheduler.id.trace | string, ro | Get a worker trace. |
| scheduler.zone | int, ro | Currently active compaction redzone. |
| scheduler.checkpoint\_active | int, ro | Shows if checkpoint operation is in progress. |
| scheduler.checkpoint\_lsn | int, ro | LSN of currently active checkpoint operation. |
| scheduler.checkpoint\_lsn\_last | int, ro | LSN of the last completed checkpoint operation. |
| scheduler.checkpoint | function | Force to start background checkpoint. Does not block. |
| scheduler.anticache\_active | int, ro | Shows if anti-cache operation is in progress. |
| scheduler.anticache\_asn | int, ro | ASN of currently active anti-cache operation. |
| scheduler.anticache\_asn\_last | int, ro | ASN of the last completed Anti-Cache operation. |
| scheduler.anticache | function | Force to start background anti-cache. Does not block. |
| scheduler.snapshot\_active | int, ro | Shows if snapshot operation is in progress. |
| scheduler.snapshot\_ssn | int, ro | SSN of currently active snapshot operation. |
| scheduler.snapshot\_ssn\_last | int, ro | SSN of the last completed snapshot operation. |
| scheduler.snapshot | function | Force to start background snapshot operation. Does not block. |
| scheduler.on\_recover | pointer | Set recover log function. |
| scheduler.on\_recover\_arg | pointer | on\_recover function arg. |
| scheduler.on\_event | pointer | Set a callback which will be called on an async operation completion. |
| scheduler.on\_event\_arg | pointer | on\_event function argument. |
| scheduler.event\_on\_backup | int | Generate async event on a backup completion. |
| scheduler.gc\_active | function | Shows if gc operation is in progress. |
| scheduler.gc | function | Force to start background gc. Does not block. |
| scheduler.lru\_active | function | Shows if lru operation is in progress. |
| scheduler.lru | function | Force to start background lru operation. Does not block. |
| scheduler.run | function | Run scheduler step in a current thread. For testing purposes. |
