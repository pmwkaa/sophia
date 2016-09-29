
<p align="center">
	<a href="http://sphia.org"><img src="http://sophia.systems/sophia.png" /></a><br>
</p>
<p align="center">
	<a href="http://sphia.org">sophia</a> - modern key-value / row storage library.
</p>
<br>

###design notes

Sophia is designed in a way to reduce number of internal dependencies strictly to one-way graph, in which
higher level libraries deals with lower level libraries. Low-level libraries should never access in any way to
a higher level libraries. This approach allows to keep code much simplier and cleaner. Do unit
testing layer by layer only with separate subsystems.

###engine implementation

**std** (prefix: *ss*) 

Sophia standart library implements basic types, data structures, io routines, memory allocators,
buffer, common iterator and so on.
This is the only library which has OS dependent code.

**object** (prefix: *so*) dependencies: *std*

Object library implements low-level object API interface `soif` and object pools `sopool`.
Basically this library allows to cast a pointer into a Sophia object and call a particular method. Widely
used by *environment* and *sophia* wrappers.

**format** (prefix: *sf*) dependencies: *std*

Format library implements raw data storage format `sf` and rules how to store and access data in it,
which are described by scheme `sfscheme`. Upsert callback format `sfupsert` and type limits are
also live here.

**runtime** (prefix: *sr*) dependencies: *format*

Runtime library has all the stuff needed during runtime operations `sr`. This include allocator contexts,
error contexts `srerror`, configuration parser `srconf`, logger `srlog`, sequence numbers tracker `srseq`, status `srstatus` and
statistics `srstat`.

**version** (prefix: *sv*) dependencies: *runtime*

Version library implements multi-versional logic (implemented in form of iterators) to merge row streams
`svmerge, svmergeiter` to read and filter row streams `svread` and to write row streams `svwrite`.
Additionally this library implements row format extended to be used in-memory `svv` and
in-memory index used for row storage `svindex`. Transaction log is also implemented here `svlog`.

**transaction** (prefix: *sx*) dependencies: *version*

Sophia transaction manager library deals with transactions `sx` and lock indexes `sxindex` which are
used for conflict resolution and deadlock detection. Sophia implements Serialized Snapshot Isolation (SSI)
level by default. Sophia transactions are completely in-memory. Transaction manager is separate entity (which can be easily removed),
only *environment* owns it. In other subsystems LSN number is enough to match a visible row version.

**wal** (prefix: *sw*) dependencies: *version*

Write-Ahead Log used to write logical changes to a database, before data are saved in storage. Log files
are replayed `sliter` during database recovery. Each log file structure `sl` holds reference counter describing each written statement
`slv` which still holded in-memory `svv`. When reference counter reaches zero, a log file will be automaticaly removed (garbage-collected).
This process is controlled by the *scheduler*.

**database** (prefix: *sd*) dependencies: *version*

Database library works with on-disk database file (node file) representation. It implements page format storage `sdpage` and its iterator `sdpageiter`,
index format `sdindex` and its iterator `sdindexiter`. Iterators are used for page read `sdread`, write `sdwrite` and file read
during recovery `sditer`. Additionally in-memory page builder `sdbuild`, stream merger `sdmerge` and scheme file `sdscheme` also
implemented there.

**index** (prefix: *si*) dependencies: *database*

Index `si` is a core data storage engine. Each index is associated with a database by `environment`. Index stores rows `svv` in nodes
`sinode` in-memory indexes `svindex`. Each node is associated with a database file `sdindex`. Index implements all of the parallel operations
such as compaction, backup, etc. Node `sinode` is a single unit of background work (one worker - one node). Index does not start
background work, `scheduler` is the one responsible for it. Additionally index has a planner `siplanner` which tracks memory usage per node and
capable to find next node suitable for any background task.

**scheduler** (prefix: *sc*) dependencies: *wal*, *index*

Sophia scheduler `sc` controls all of maintaince jobs run in background such as compaction, gc, log gc, rotation,
backup and so on. Additionally *wal* - *index* commit is also a part of scheduler. Scheduler is periodically called by worker
threads which are run in *environment*. Scheduler is designed to be run in a multi or single thread.
This extensively used by test suite.

**repository** (prefix: *sy*) dependencies: *runtime*

Repository `sy` is responsible for repository and backup directories open and creation.

**environment** (prefix: *se*) dependencies: *object*, *repository*, *transaction*, *scheduler*

Environment handles startup, configuration, shutdown and creation of all of the Sophia objects `soif` such as database `sedb`,
transactions `setx`, cursors `secursor`, etc. For each defined database (eg "db.test") environment creates
`sedb` object and associates `si` and `sxindex` objects.
During recovery, environment does si_open() for each database and WAL replay afterwards. After startup is complete, environment
runs a scheduler and a worker pool.

**sophia** (prefix: *sp*) dependencies: *object*, *environment*

Home for Sophia API methods (main).
