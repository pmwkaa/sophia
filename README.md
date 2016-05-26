
<p align="center">
	<a href="http://sphia.org"><img src="http://sophia.systems/sophia.png" /></a><br>
</p>

### Sophia

[Sophia](http://sophia.systems) is advanced MVCC transactional key-value/row storage library.

Write and Range Scan optimized. It can efficiently work with ordered data, such
as a time-series, event-storage, log-storage, server metrics, etc.

**Features**

* Full [ACID](http://en.wikipedia.org/wiki/ACID) compliancy
* [Multi-Version Concurrency Control (MVCC)](http://en.wikipedia.org/wiki/Multiversion_concurrency_control) engine
* Pure Append-Only
* Multi-threaded (Both client access and near-linear [compaction scalability](http://sophia.systems/v2.1/admin/compaction.html))
* [Multi-databases](admin/database.md) support (single environment and WAL)
* Secondary indexes
* Multi-Statement and Single-Statement [Transactions](http://sophia.systems/v2.1/crud/transactions.html) (multi-databases)
* Serialized Snapshot Isolation (SSI)
* Persistent [RAM Storage](http://sophia.systems/v2.1/admin/ram.html) mode
* [Anti-Cache](http://sophia.systems/v2.1/admin/anticache.html) Storage mode
* [LRU](http://sophia.systems/v2.1/admin/lru.html) Storage
* Optional [AMQ Filter](http://sophia.systems/v2.1/admin/amqf.html) (approximate member query filter) based on Quotient Filter
* [Upsert](http://sophia.systems/v2.1/crud/upsert.html): optimized write-only 'update or insert' operation
* Consistent [Cursors](http://sophia.systems/v2.1/crud/cursors.html)
* [Prefix search](http://sophia.systems/v2.1/crud/cursors.html)
* [Duplicate compression](http://sophia.systems/v2.1/admin/compression.html)
* Point-in-Time [Views](http://sophia.systems/v2.1/admin/view.html)
* Online/Versional database [creation](http://sophia.systems/v2.1/admin/database.html) and asynchronous [shutdown/drop](http://sophia.systems/v2.1/admin/database.html)
* Asynchronous [Online/Hot Backup](admin/backup.md)
* [Compression](http://sophia.systems/v2.1/admin/compression.html) (no blocks, no-holes, supported: lz4, zstd)
* [Compression](http://sophia.systems/v2.1/admin/compression.html) for hot and cold data (distinct compression types)
* Optimizations for [faster recovery](http://sophia.systems/v2.1/admin/snapshot.html) with large datasets (snapshot)
* Easy to use ([Minimalistic API](http://sophia.systems/v2.1/tutorial/api.html))
* Easy to write bindings (FFI-friendly)
* Easy to [built-in](http://sophia.systems/v2.1/tutorial/build.html) (amalgamated source)
* Event loop friendly
* Implemented as small *C-written* [library](http://sophia.systems/v2.1/tutorial/build.html) with zero dependencies
* Carefully tested
* Open Source Software, [*BSD* Licensed](http://sophia.systems/v2.1/tutorial/license.html)

### Quickstart

* **Documentation** to get started, administration and API are available [Here](http://sophia.systems/v2.1/index.html).
* **Examples** Quick GitHub examples are [Here](https://github.com/pmwkaa/sophia/tree/master/example).
* **Bindings** for the most common languages supported by Community are [Here](http://sophia.systems/drivers.html).

### Support

Please use Official Sophia [Google Group](http://groups.google.com/group/sophia-database) or
[StackOverflow](http://stackoverflow.com/tags/sophia) to ask any general questions.<br>
More information is available [Here](http://sophia.systems/support.html).
<br><br>

<a href="https://travis-ci.org/pmwkaa/sophia"><img src="https://travis-ci.org/pmwkaa/sophia.svg?branch=master" /></a>
<a href="https://scan.coverity.com/projects/5109"><img src="https://scan.coverity.com/projects/5109/badge.svg" /></a>
<a href="https://coveralls.io/r/pmwkaa/sophia?branch=master"><img src="https://coveralls.io/repos/pmwkaa/sophia/badge.svg?branch=master" /></a>
