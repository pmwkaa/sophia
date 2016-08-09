
Memory-Mapped Mode
------------------

By default Sophia uses pread(2) to read data from disk. Using mmap mode, Sophia handles all requests by
directly accessing memory-mapped node files memory.

Following variable can be set to enable or disable mmap mode: **db.database_name.mmap**

```C
sp_setint(env, "db.test.mmap", 1);
```

It is a good idea to try this mode, even if your dataset is rather small or you need to handle
a large ratio of read request with a predictable pattern.

Disadvantage of mmap mode, in comparison to RAM Storage, is a possible unpredictable
latency behaviour and a OS cache warmup period after recovery.
