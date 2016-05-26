
<p align="center">
	<a href="http://sphia.org"><img src="http://sophia.systems/sophia.png" /></a><br>
</p>
<br>
[Sophia](http://sophia.systems) is advanced transactional [MVCC](http://en.wikipedia.org/wiki/Multiversion_concurrency_control)
key-value/row storage library.

Optimized for Write and Range-Query cases. It can efficiently work with ordered data, such as a time-series,
event-storage, log-storage, server metrics, etc.

**Features**

* Full [ACID](http://en.wikipedia.org/wiki/ACID) compliancy
* [Multi-Version Concurrency Control (MVCC)](http://en.wikipedia.org/wiki/Multiversion_concurrency_control) engine
* Pure Append-Only
* Multi-threaded (api and [compaction scalability](documentation/admin/compaction.md))
* [Multi-databases](documentation/admin/database.md) support (sharing write-ahead log)
* Secondary indexes
* Multi-Statement and Single-Statement [Transactions](documentation/crud/transactions.md) (multi-databases)
* Serialized Snapshot Isolation (SSI)
* Persistent [RAM Storage](documentation/admin/ram.md) mode
* [Anti-Cache](documentation/admin/anticache.md) Storage mode
* [LRU](documentation/admin/lru.md) Storage
* Optional [AMQ Filter](documentation/admin/amqf.md) (approximate member query filter) based on Quotient Filter
* [Upsert](documentation/crud/upsert.md): optimized write-only 'update or insert' operation
* Consistent [Cursors](documentation/crud/cursors.md)
* [Prefix search](documentation/crud/cursors.md)
* [Duplicate compression](documentation/admin/compression.md)
* Automatic garbage-collection
* Automatic key-expire
* Point-in-Time [Views](documentation/admin/view.md)
* Online/Versional database [creation](documentation/admin/database.md) and asynchronous [shutdown/drop](documentation/admin/database.md)
* Asynchronous [Online/Hot Backup](documentation/admin/backup.md)
* [Compression](documentation/admin/compression.md) (no blocks, no-holes, supported: lz4, zstd)
* [Compression](documentation/admin/compression.md) for hot and cold data (distinct compression types)
* Optimizations for [faster recovery](documentation/admin/snapshot.md) with large datasets (snapshot)
* Easy to use ([Minimalistic API](documentation/tutorial/api.md))
* Easy to write bindings (FFI-friendly)
* Easy to [built-in](documentation/tutorial/build.md) (amalgamated source)
* Event loop friendly
* Implemented as small *C-written* [library](documentation/tutorial/build.md) with zero dependencies
* Carefully tested
* Open Source Software, [*BSD* Licensed](documentation/tutorial/license.md)

**Support**

Sophia [Documentation](http://sophia.systems/v2.1/index.html) and [Bindings](http://sophia.systems/drivers.html)
for the most common languages are available on the [website](http://sophia.systems).

Please use Official Sophia [Google Group](http://groups.google.com/group/sophia-database) or
[StackOverflow](http://stackoverflow.com/tags/sophia) to ask any general questions.<br>
More information is available [Here](http://sophia.systems/support.html).
<br><br>

<a href="https://travis-ci.org/pmwkaa/sophia"><img src="https://travis-ci.org/pmwkaa/sophia.svg?branch=master" /></a>
<a href="https://scan.coverity.com/projects/5109"><img src="https://scan.coverity.com/projects/5109/badge.svg" /></a>
<a href="https://coveralls.io/r/pmwkaa/sophia?branch=master"><img src="https://coveralls.io/repos/pmwkaa/sophia/badge.svg?branch=master" /></a>
