
Compression
-----------

Following options can be set to enable or disable compression usage: **db.database_name.compression**
and **db.database_name.compression_branch**.

It is possible to choose different compression types for Cold and Hot data (in terms of updates).

Most of data are stored in Cold branches (*compression*).
While Hot data stored in a recently created branches (*compression\_branch*).

```C
sp_setstring(env, "db.test.compression", "lz4", 0);
sp_setstring(env, "db.test.compression_branch", "lz4", 0);
```

Supported compression values: **lz4**, **zstd**, **none** (default).

RAM Compression
---------------

It is possible to enable and combine [compression](compression.md) and [RAM Storage](ram.md) Mode.

Prefix Compression
------------------

Prefix compression is implemented by compressing multi-part keys duplicates
during compaction process.

To enable key compression:
```C
sp_setint(env, "db.test.compression_key", 1);
```
