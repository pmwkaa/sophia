
DBMS Integration
----------------

Sophia support special work modes which can be used to support
external Write-Ahead Log: **log.enable** and **sophia.recover**.

Two-Phase Recover
-----------------

In this mode Sophia processes transactions to ensure that they were not
already commited.

After second [sp_open()](../api/sp_open.md) Sophia starts environment.

1. env = [sp_env()](../api/sp_env.md)
2. sophia.recover = **2**
3. log.enable = 0
4. [sp_open(env)](../api/sp_open.md) start in recovery mode, compaction not started
  1. start defining and recovering databases
  2. start replaying transactions from external WAL
  3. [sp_setint(transaction, "lsn", lsn)](../api/sp_setint.md) forge transaction *lsn* before commit
  4. [sp_commit(transaction)](../api/sp_commit.md) every commit ensures that data were not previously written to disk
5. [sp_open(env)](../api/sp_open.md) *second time* starts in default mode

This mode can be helpful for Sophia integration into other database management system, which
supports its own Write-Ahead Log.

N-Phase Recover
---------------

This recovery mode allows to switch Sophia into *recovery* mode and back on the fly.

1.  env = [sp_env()](../api/sp_env.md)
2.  sophia.recover = **3**
3.  log.enable = 0
4.  [sp_open(env)](../api/sp_open.md) start in **default** mode with thread-pool run.
5.  usual transaction processing
6.  [sp_open(env)](../api/sp_open.md) switch to *recovery* mode
7.  start replaying transactions from external *source*
8.  [sp_setint(transaction, "lsn", lsn)](../api/sp_setint.md) forge transaction *lsn* before commit
9.  [sp_commit(transaction)](../api/sp_commit.md) every commit ensures that data were not previously written to disk
10. [sp_open(env)](../api/sp_open.md) *again* switch Sophia back to default mode.

Steps from 4-9 can be repeated any time.

This mode can be helpful for Sophia integration with most of a Replication/JOIN
technologies.
