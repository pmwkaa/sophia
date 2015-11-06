
# sophia test-suite

STS_TESTS = unit/ss_leb128.test.o \
            unit/ss_pager.test.o \
            unit/ss_a.test.o \
            unit/ss_aslab.test.o \
            unit/ss_order.test.o \
            unit/ss_rq.test.o \
            unit/ss_ht.test.o \
            unit/ss_zstdfilter.test.o \
            unit/ss_lz4filter.test.o \
            unit/sr_conf.test.o \
            unit/sr_scheme.test.o \
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
            generic/cache.test.o \
            generic/error.test.o \
            generic/method.test.o \
            generic/profiler.test.o \
            generic/repository.test.o \
            generic/shutdown.test.o \
            generic/drop.test.o \
            generic/ddl.test.o \
            generic/multipart.test.o \
            generic/tpr.test.o \
            generic/object.test.o \
            generic/env.test.o \
            generic/deadlock.test.o \
            generic/scheme.test.o \
            generic/rev.test.o \
            generic/branch.test.o \
            generic/compact.test.o \
            generic/backup.test.o \
            generic/checkpoint.test.o \
            generic/db_cursor.test.o \
            generic/gc.test.o \
            generic/prefix.test.o \
            generic/transaction_md.test.o \
            generic/cursor_md.test.o \
            generic/half_commit.test.o \
            generic/update.test.o \
            generic/async.test.o \
            generic/get_cache.test.o \
            functional/hermitage.test.o \
            functional/transaction.test.o \
            functional/cursor.test.o \
            crash/durability.test.o \
            crash/workflow.test.o \
            crash/oom.test.o \
            crash/io.test.o \
            crash/recover_loop.test.o \
            multithread/multithread.test.o \
            multithread/multithread_be.test.o \
            memory/leak.test.o

SOPHIA_DIRS    = std format runtime object version log database \
                 index \
                 transaction \
                 repository \
                 environment \
                 sophia
SOPHIA_INCLUDE = $(addprefix -I../sophia/, $(SOPHIA_DIRS))
ifdef ENABLE_COVERAGE
STS_COVERAGE   = --coverage
else
STS_COVERAGE   =
endif
STS_CFLAGS     = $(SOPHIA_INCLUDE) -g -O0 -Wall -I. -I../sophia -Isuite/ -pthread
STS_LDFLAGS    = ../sophia.o suite/libst.a -pthread -lm $(STS_COVERAGE)
STS_BINARY     = sophia-test
STS_SOPHIA_DIR = _test_sophia
STS_BACKUP_DIR = _test_backup
STS_LOG_DIR    = _test_log
STS_DB_DIR     = _test_db

all: banner validate sophia-test
sophia-test: suite $(STS_TESTS)
	@echo "ld $(STS_BINARY)"
	@$(CC) $(STS_TESTS) sophia-test.c $(STS_CFLAGS) $(STS_LDFLAGS) -o $(STS_BINARY)
suite:
	@(cd suite; make --silent --no-print-directory)
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
	@echo "cc $<"
	@$(CC) $(STS_CFLAGS) -c $< -o $@
clean:
	@(cd suite; make --no-print-directory clean)
	@rm -f $(STS_BINARY)
	@rm -f $(STS_TESTS)
	@rm -fr $(STS_SOPHIA_DIR) \
	        $(STS_BACKUP_DIR) \
	        $(STS_DB_DIR) $(STS_LOG_DIR)
test:
	@gdb -q -return-child-result -batch -ex "run" -ex "bt" --args $(STS_BINARY) -v

.PHONY: all clean validate banner suite test
.PHONY: sophia-test

# vim: syntax=make
