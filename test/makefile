
# sophia test-suite

STS_TESTS = unit/ss_a.test.o \
            unit/ss_order.test.o \
            unit/ss_rq.test.o \
            unit/ss_qf.test.o \
            unit/ss_ht.test.o \
            unit/ss_zstdfilter.test.o \
            unit/ss_lz4filter.test.o \
            unit/sf_scheme.test.o \
            unit/sr_conf.test.o \
            unit/sv_v.test.o \
            unit/sv_index.test.o \
            unit/sv_indexiter.test.o \
            unit/sv_mergeiter.test.o \
            unit/sv_writeiter.test.o \
            unit/sl.test.o \
            unit/sl_iter.test.o \
            unit/sd_build.test.o \
            unit/sd_v.test.o \
            unit/sd_read.test.o \
            unit/sd_pageiter.test.o \
            generic/conf.test.o \
            generic/error.test.o \
            generic/method.test.o \
            generic/profiler.test.o \
            generic/repository.test.o \
            generic/ddl.test.o \
            generic/multipart.test.o \
            generic/document.test.o \
            generic/reuse.test.o \
            generic/env.test.o \
            generic/deadlock.test.o \
            generic/scheme.test.o \
            generic/rev.test.o \
            generic/backup.test.o \
            generic/snapshot.test.o \
            generic/checkpoint.test.o \
            generic/prefix.test.o \
            generic/transaction_batch.test.o \
            generic/transaction_md.test.o \
            generic/transaction_misc.test.o \
            generic/cursor_cache.test.o \
            generic/cursor_md.test.o \
            generic/upsert.test.o \
            generic/get_cold.test.o \
            generic/amqf.test.o \
            generic/secondary_index.test.o \
            issues/github.test.o \
            compaction/log.test.o \
            compaction/branch.test.o \
            compaction/compact.test.o \
            compaction/compact_delete.test.o \
            compaction/gc.test.o \
            compaction/expire.test.o \
            compaction/lru.test.o \
            cache/anticache.test.o \
            functional/hermitage.test.o \
            functional/transaction.test.o \
            functional/cursor.test.o \
            crash/durability.test.o \
            crash/workflow.test.o \
            crash/oom.test.o \
            crash/io.test.o \
            crash/recover_loop.test.o \
            multithread/multithread.test.o \
            multithread/multithread_upsert.test.o \
            multithread/multithread_be.test.o \
            memory/leak.test.o

SOPHIA_DIRS    = std format runtime object version log database \
                 index \
                 transaction \
                 repository \
                 scheduler \
                 environment \
                 sophia
SOPHIA_INCLUDE = $(addprefix -I../sophia/, $(SOPHIA_DIRS))

ifdef ENABLE_COVERAGE
STS_COVERAGE   = --coverage
else
STS_COVERAGE   =
endif
STS_CFLAGS     = $(SOPHIA_INCLUDE) -g -O0 -Wall -I. -I../sophia -Isuite/ -pthread
STS_LDFLAGS    = ../sophia.o -pthread -lm -lrt $(STS_COVERAGE)
STS_OBJECTS    = suite/st_r.o \
                 suite/st_suite.o \
                 suite/st_generator.o \
                 suite/st.o \
                 suite/st_document.o \
                 suite/st_tools.o \
                 suite/st_scenes.o \
                 suite/st_phase.o
STS_BINARY     = sophia-test
STS_PROF       = sophia-prof
STS_SOPHIA_DIR = _test_sophia
STS_BACKUP_DIR = _test_backup
STS_LOG_DIR    = _test_log
STS_DB_DIR     = _test_db

all: banner sophia-test sophia-prof
sophia-test: validate $(STS_OBJECTS) $(STS_TESTS)
	@echo "ld $(STS_BINARY)"
	@$(CC) $(STS_TESTS) sophia-test.c $(STS_CFLAGS) $(STS_LDFLAGS) $(STS_OBJECTS) -o $(STS_BINARY)
sophia-prof: validate $(STS_OBJECTS)
	@echo "ld $(STS_PROF)"
	@$(CC) sophia-prof.c $(STS_CFLAGS) $(STS_LDFLAGS) -o $(STS_PROF)
banner:
	@echo
	@echo sophia test-suite.
	@echo
validate:
	@if [ ! -f ../sophia.o ]; then \
		echo "Please build sophia first."; \
		echo ""; \
		exit 1; \
	fi
.c.o:
	@echo "$(CC) $<"
	@$(CC) $(STS_CFLAGS) -c $< -o $@
clean:
	@rm -f $(STS_BINARY)
	@rm -f $(STS_PROF)
	@rm -f $(STS_TESTS)
	@rm -f $(STS_OBJECTS)
	@rm -fr $(STS_SOPHIA_DIR) \
	        $(STS_BACKUP_DIR) \
	        $(STS_DB_DIR) $(STS_LOG_DIR)
test_gdb:
	@gdb -q -return-child-result -batch -ex "run" -ex "bt" --args $(STS_BINARY) -v
test:
	@./$(STS_BINARY) -v

.PHONY: all clean validate banner test
.PHONY: sophia-test sophia-prof

# vim: syntax=make
