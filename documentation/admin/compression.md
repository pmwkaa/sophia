
Compression
-----------

Following options can be set to enable or disable compression usage: **db.database_name.compression**.

```C
sp_setstring(env, "db.test.compression", "lz4", 0);
```

Supported compression values: **lz4**, **zstd**, **none** (default).
