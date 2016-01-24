
LRU Mode
--------

LRU stands for 'Least Recently Used' [eviction](https://en.wikipedia.org/wiki/Cache_algorithms) algorithm.

When LRU mode is enabled, Sophia tries to maintain a database size limit by evicting
oldest records. LSN number is used for eviction, which happens during compaction.

Following variable can be set to enable and set database size limit: **db.database_name.lru**

```C
/* limit database size by 1Gb */
sp_setint(env, "db.test.lru", 1 * 1024 * 1024 * 1024);
```

LRU mode should be used in conjunction with [Persistent Caching](cache.md) mode.
