
Database Scheduler
------------------

| name | type | description  |
|---|---|---|
| db.name.scheduler.checkpoint | int, ro | Shows if checkpoint operation is in progress. |
| db.name.scheduler.checkpoint\_lsn | int, ro | LSN of currently active checkpoint operation. |
| db.name.scheduler.checkpoint\_lsn\_last | int, ro | LSN of the last completed checkpoint operation. |
| db.name.scheduler.snapshot | int, ro | Shows if snapshot operation is in progress. |
| db.name.scheduler.snapshot\_ssn | int, ro | SSN of currently active snapshot operation. |
| db.name.scheduler.snapshot\_ssn\_last | int, ro | SSN of the last completed snapshot operation. |
| db.name.scheduler.gc | int, ro | Shows if gc operation is in progress. |
| db.name.scheduler.expire | int, ro | Shows if expire operation is in progress. |
| db.name.scheduler.backup | int, ro | Shows if backup operation is in progress. |
