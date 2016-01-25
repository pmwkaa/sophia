
<p align="center">
	<a href="http://sphia.org"><img src="http://sophia.systems/sophia.png" /></a><br>
	<a href="http://sphia.org"><img src="http://sophia.systems/logo_title.png" /></a>
</p>

# Sophia 2.1

[Sophia](http://sophia.systems) is advanced Embeddable Transactional Key-Value Storage.
Open-Source, available free of charge under terms of [BSD License](http://sophia.systems/v2.1/tutorial/license.html).

Can be used in form of shared library or integrated within a project.<br>
Written in C with zero dependencies. 

### RAM-Disk Hybrid

Sophia has unique hybrid architecture that was specifically designed to efficiently store data
using the combinations of Disk, Flash and RAM.

Sophia allows to distinct Hot (read/write-intensive) and Cold data.

Supported storage modes are:

* **Disk Storage** Disk or Flash is the primary storage. Write requests are cached in RAM for futher compaction.
* **Anti-Caching** RAM becomes the primary storage for Hot data. Cold data stored on Disk or Flash.
* **Persistent Caching** Secondary storage used as an LRU cache for Hot data stored in RAM or Flash.
Cold and Hot data persisted on Disk or Flash.
* **Persistent RAM Storage** RAM is the primary storage. Data continiously persisted on Disk or Flash.

The MVCC Append-Only data storage engine was created as a result of research and reconsideration
primary algorithmic constraints of Log-file based data structures.

Write and Range Scan optimized. It can efficiently work with large data sets.

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
