
<center>
<img src="sophia.png"></img>
</center>

<br>

<center>
<img src="title.png"></img>
</center>

<br>

# Sophia 2.1 Manual

Welcome to the Sophia 2.1 Manual.

Sophia is advanced Embeddable Transactional Key-Value Storage. Open-Source, available
free of charge under terms of [BSD License](tutorial/license.md).

This manual is also available in [PDF](sophia_v21_manual.pdf) <i class="fa fa-file-pdf-o"></i>.

### RAM-Disk Hybrid

Sophia has unique hybrid architecture that was specifically designed
to efficiently store data using the combination of HDD, Flash and RAM.

Sophia allows to distinct Hot (read-intensive) and Cold data.

The data storage engine was created as a result of research and reconsideration
primary algorithmic constraints of Log-file based data structures.

Write and Range Scan optimized. It can efficiently work with terrabyte-sized datasets.

### Language bindings

Bindings for the most common languages are available [here](http://sophia.systems/drivers.html).

### v2.1 features

* Full [ACID](http://en.wikipedia.org/wiki/ACID) compliancy
* [Multi-Version Concurrency Control (MVCC)](http://en.wikipedia.org/wiki/Multiversion_concurrency_control) engine
* Pure Append-Only
* Multi-threaded (Both client access and near-linear [compaction scalability](admin/compaction.md))
* [Multi-databases](admin/database.md) support (Single environment and WAL)
* Multi-Statement and Single-Statement [Transactions](crud/transactions.md) (Multi-databases)
* Serialized Snapshot Isolation (SSI)
* Persistent [RAM Storage](admin/ram.md) mode
* Persistent [Caching](admin/cache.md) mode
* [Anti-Cache](admin/anticache.md) Storage mode
* [LRU](admin/lru.md) Storage
* Separate storage formats: key-value (default), document (keys are part of value)
* Optional [AMQ Filter](admin/amqf.md) (Approximate member query filter) based on Quotient Filter
* Async and sync reads (Callback triggered vs. blocking)
* [Upsert](crud/upsert.md): optimized write-only 'Update or Insert' operation
* Consistent [Cursors](crud/cursors.md)
* [Prefix search](crud/cursors.md)
* [Prefix compression](admin/compression.md) (Using key-part duplicates)
* Point-in-Time [Views](admin/view.md)
* Online/Versional database [creation](admin/database.md) and asynchronous [shutdown/drop](admin/database.md)
* Asynchronous [Online/Hot Backup](admin/backup.md)
* [Compression](admin/compression.md) (Per region, no-holes, supported: lz4, zstd)
* [Compression](admin/compression.md) for Hot and Cold data (distinct compression types)
* Meta-data Compession (By default)
* Optimizations for [faster recovery](admin/snapshot.md) with big databases (Snapshot)
* Easy to use ([Minimalistic API](tutorial/api.md))
* Easy to [integrate](admin/integration.md) into a DBMS (Native support of using as storage engine)
* Easy to write bindings (FFI-friendly, API designed to be stable in future)
* Easy to [built-in](tutorial/build.md) (Amalgamated, compiles into two C files)
* Event loop friendly
* Zero-Configuration (Tuned by default)
* Implemented as small *C-written* [library](tutorial/build.md) with zero dependencies
* Carefully tested
* Open Source Software, [*BSD* Licensed](tutorial/license.md)
