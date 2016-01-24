
Database
--------

To create a database, new database name should be set to **db** control namespace.
If no database exists, it will be created automatically.

Database can be created, opened or deleted before or after environment startup.

Database has following states: offline, online, recover, shutdown, malfunction.
Database sets malfunction status if any unrecoverable error occurs.

| name | type | description  |
|---|---|---|
| db.name.name | string, ro | Get database name |
| db.name.id | int | Database's sequential id number. This number is used in the transaction log for the database identification. |
| db.name.status | string, ro | Get database status. |
| db.name.storage | string | Set storage mode: anti-cache, cache, in-memory. |
| db.name.format | string | Set database format: kv, document. |
| db.name.amqf | int | enable or disable AMQ Filter. |
| db.name.path | string | Set folder to store database data. If variable is not set, it will be automatically set as **sophia.path/database_name**. |
| db.name.path\_fail\_on\_exists | int | Produce error if path already exists. |
| db.name.path\_fail\_on\_drop | int | Produce error on attempt to open 'dropped' database directory. |
| db.name.cache\_mode | int | Mark this database as a cache. |
| db.name.cache | string | Set name of a cache database to use. |
| db.name.mmap | int | Enable or disable mmap mode. |
| db.name.sync | int | Sync node file on the branch creation or compaction completion. |
| db.name.node\_preload | int | Preload whole node into memory for compaction. |
| db.name.node\_size | int | Set a node file size in bytes. Node file can grow up to two times the size before the old node file is being split. |
| db.name.page\_size | int | Set size of a page to use. |
| db.name.page\_checksum | int | Check checksum during compaction. |
| db.name.compression\_key | int | Enable or disable prefix (multi-part) compression. |
| db.name.compression | string | Specify compression driver. Supported: lz4, zstd, none (default). |
| db.name.compression\_branch | string | Specify compression driver for branches. |
| db.name.lru | int | Enable LRU mode. |
| db.name.lru\_step | int | Set LRU accuracy. |
| db.name.branch | function | Force branch creation. |
| db.name.compact | function | Force compaction. |
| db.name.compact\_index | function | Force two-level compaction. |
| db.name.index.memory\_used | int, ro | Memory used by database for in-memory key indexes in bytes. |
| db.name.index.size | int, ro | Sum of nodes size in bytes (compressed). This is equal to the full database size. |
| db.name.index.size\_uncompressed | int, ro | Full database size before the compression. |
| db.name.index.size\_snapshot | int, ro | Snapshot file size. |
| db.name.index.size\_amqf | int, ro | Total size used by AMQ Filter. |
| db.name.index.count | int, ro | Total number of keys stored in database. This includes transactional duplicates and not yet-merged duplicates. |
| db.name.index.count\_dup | int, ro | Total number of transactional duplicates. |
| db.name.index.read\_disk | int, ro | Number of disk reads since start. |
| db.name.index.read\_cache | int, ro | Number of cache reads since start. |
| db.name.index.temperature\_avg | int, ro | Average index temperature. |
| db.name.index.temperature\_min | int, ro | Min index node temperature. |
| db.name.index.temperature\_max | int, ro | Max index node temperature. |
| db.name.index.temperature\_histogram | string, ro | Index temperature distribution histogram. |
| db.name.index.node\_count | int, ro | Number of active nodes. |
| db.name.index.branch\_count | int, ro | Total number of branches. |
| db.name.index.branch\_avg | int, ro | Average number of branches per node. |
| db.name.index.branch\_max | int, ro | Maximum number of branches per node. |
| db.name.index.branch\_histogram | string, ro | Branch histogram distribution through all nodes. |
| db.name.index.page\_count | int, ro | Total number of pages. |
| db.name.index.upsert | function | Set upsert callback function. |
| db.name.index.upsert\_arg | function | Set upsert function argument. |
| db.name.index.key | string | Set index key type (string, u32, u64, u32\_rev, u64\_rev). See database section for details. |
