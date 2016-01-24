
Backup
------

| name | type | description  |
|---|---|---|
| backup.path | string | Set backup path. Each new backup will create a **backup.path/id** folder containing complete environment copy. |
| backup.run | function | Start background backup. Does not block. |
| backup.active | int | Shows if backup operation is in progress. |
| backup.last | int | Shows id of the last completed backup. |
| backup.last\_complete | int | Shows if the last backup was successful. |
