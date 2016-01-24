
Write Ahead Log
---------------

| name | type | description  |
|---|---|---|
| log.enable | int | Enable or disable transaction log. |
| log.path | string | Set folder for transaction log directory. If variable is not set, it will be automatically set as **sophia.path/log**. |
| log.sync | int | Sync transaction log on every commit. |
| log.rotate\_wm | int | Create new log file after rotate\_wm updates. |
| log.rotate\_sync | int | Sync log file on every rotation. |
| log.rotate | function | Force to rotate log file. |
| log.gc | function | Force to garbage-collect log file pool. |
| log.files | int, ro | Number of log files in the pool. |
