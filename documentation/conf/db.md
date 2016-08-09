
Database
--------

To create a database, new database name should be set to **db** control namespace.
If no database exists, it will be created automatically.

Database can be created, opened or deleted only before environment startup.

| name | type | description  |
|---|---|---|
| db.name.name | string, ro | Get database name |
| db.name.id | int | Database's sequential id number. This number is used in the transaction log for the database identification. |
| db.name.path | string | Set folder to store database data. If variable is not set, it will be automatically set as **sophia.path/database_name**. |
| db.name.memory\_limit | int | Set memory limit in bytes (0 - disabled). |
| db.name.mmap | int | Enable or disable mmap mode. |
| db.name.direct\_io | int | Enable or disable O\_DIRECT mode. |
| db.name.sync | int | Sync node file on the branch creation or compaction completion. |
| db.name.temperature | int | Track index temperature. |
| db.name.amqf | int | Enable or disable AMQ Filter. |
| db.name.expire | int | Enable or disable key expire. |
| db.name.compression\_cold | string | Specify compression driver. Supported: lz4, zstd, none (default). |
| db.name.compression\_hot | string | Specify compression driver for branches. |
| db.name.comparator | function | Set custom comparator function (example: [comparator.c](https://github.com/pmwkaa/sophia/blob/master/example/comparator.c)). |
| db.name.comparator\_arg | string | Set custom comparator function arg. |
| db.name.upsert | function | Set upsert callback function. |
| db.name.upsert\_arg | string | Set upsert function argument. |
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
