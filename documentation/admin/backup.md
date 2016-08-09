
Backup and Restore
------------------

Sophia supports asynchronous Hot/Online Backups.

Each backup iteration creates exact copy of environment, then assigns backup sequential number.
Sophia v2.2 does not support incremental backup.

**backup.path** must be set with a specified folder which will contain resulting backup folders.
To start a backup, user must initiate **backup.run** procedure first.
Procedure call is fast and does not block.

```C
sp_setint(env, "backup.run", 0);
```

**backup.active** and **backup.last_complete** variables can be
examined to see if backup process is in progress or being succesfully completed.

To restore from a backup, a suitable backup version should be picked, copied and used
as **sophia.path** directory.
