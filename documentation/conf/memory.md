
Memory Control
--------------

Current memory limit is in bytes. This limit applies only to
memory used for storing in-memory keys. This does not limit any memory used for
node bufferization at the moment.

Memory pager is a part of Sophia that is used for memory allocation as a part of
internal slab-allocator. Used for object allocations.

Please consider to read Architecture section about memory limits.

| name | type | description  |
|---|---|---|
| memory.limit | int | Set current memory limit in bytes. |
| memory.used | int, ro | Get current memory usage in bytes. |
| memory.anticache | int | Set current memory limit for Anti-Cache mode. |
| memory.pager\_pool\_size | int, ro | Get current memory pager pool size. |
| memory.pager\_page\_size | int, ro | Get pager page size. |
| memory.pager\_pools | int, ro | Get a number of allocated pager memory pools. |
