
Compression
-----------

Following options can be set to enable or disable compression usage: **db.database_name.compression_cold**
and **db.database_name.compression_hot**.

It is possible to choose different compression types for Cold and Hot data (in terms of updates).

Most of data are stored in Cold branches (*compression_cold*).
While Hot data stored in a recently created branches (*compression\_hot*).

```C
sp_setstring(env, "db.test.compression_cold", "lz4", 0);
sp_setstring(env, "db.test.compression_hot", "lz4", 0);
```

Supported compression values: **lz4**, **zstd**, **none** (default).
