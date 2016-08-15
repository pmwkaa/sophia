
<p align="center">
	<a href="http://sphia.org"><img src="http://sophia.systems/sophia.png" /></a><br>
</p>
<br>
[Sophia](http://sophia.systems) is advanced transactional [MVCC](http://en.wikipedia.org/wiki/Multiversion_concurrency_control)
key-value/row storage library.

**How does it differ from other storages?**

Sophia is RAM-Disk hybrid storage. It is designed to provide best possible on-disk performance without degradation
in time. It has guaranteed *O(1)* worst case complexity for read, write and range scan operations.

It adopts to expected write rate, total capacity and cache size.

**What is it good for?**

For server environment, which requires
lowest latency write and read, predictable behaviour, optimized storage schema and transaction guarantees.
It can efficiently work with large volumes of ordered data, such as a time-series,
analytics, events, logs, counters, metrics, full-text search, common key-value, etc.

**Features**

* Full ACID compliancy
* MVCC engine
* Optimistic, non-blocking concurrency with N-writers and M-readers
* Pure Append-Only
* Unique data storage architecture
* Fast: O(1) worst for read, write and range scan operations
* Multi-threaded compaction
* Multi-databases support (sharing a single write-ahead log)
* Multi-Statement and Single-Statement Transactions (cross-database)
* Serialized Snapshot Isolation (SSI)
* Optimized storage schema (numeric types has zero-cost storage)
* Can be used to build Secondary Indexes
* Upsert (fast write-only 'update or insert' operation)
* Consistent Cursors
* Prefix search
* Automatic garbage-collection
* Automatic key-expire
* Hot Backup
* Compression (no fixed-size blocks, no-holes, supported: lz4, zstd)
* Direct IO support
* Use mmap or pread access methods
* Simple and easy to use (minimalistic API, FFI-friendly, amalgamated)
* Implemented as small *C-written* library with zero dependencies
* Carefully tested
* Open Source Software, BSD

**Support**

Sophia [Documentation](http://sophia.systems/v2.2/index.html) and [Bindings](http://sophia.systems/drivers.html)
for the most common languages are available on the [website](http://sophia.systems).

Please use Official Sophia [Google Group](http://groups.google.com/group/sophia-database) or
[StackOverflow](http://stackoverflow.com/tags/sophia) to ask any general questions.<br>
More information is available [Here](http://sophia.systems/support.html).
<br><br>

<a href="https://travis-ci.org/pmwkaa/sophia"><img src="https://travis-ci.org/pmwkaa/sophia.svg?branch=master" /></a>
<a href="https://scan.coverity.com/projects/5109"><img src="https://scan.coverity.com/projects/5109/badge.svg" /></a>
<a href="https://coveralls.io/r/pmwkaa/sophia?branch=master"><img src="https://coveralls.io/repos/pmwkaa/sophia/badge.svg?branch=master" /></a>
