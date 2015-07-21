
# sophia test-suite
#

OBJECTS = ss_leb128.test.o \
          ss_pager.test.o \
          ss_a.test.o \
          ss_aslab.test.o \
          ss_order.test.o \
          ss_rq.test.o \
          ss_ht.test.o \
          ss_zstdfilter.test.o \
          ss_lz4filter.test.o \
          sr_meta.test.o \
          sr_scheme.test.o \
          sv_v.test.o \
          sv_index.test.o \
          sv_indexiter.test.o \
          sv_mergeiter.test.o \
		  sv_writeiter.test.o \
          sl.test.o \
          sl_iter.test.o \
          sd_build.test.o \
          sd_v.test.o \
          sd_iter.test.o \
          sd_pageiter.test.o \
          meta.test.o \
          cache.test.o \
          error.test.o \
          method.test.o \
          profiler.test.o \
          repository.test.o \
          shutdown.test.o \
          drop.test.o \
          ddl.test.o \
          multipart.test.o \
          tpr.test.o \
          object.test.o \
          env.test.o \
          deadlock.test.o \
          scheme.test.o \
          branch.test.o \
          compact.test.o \
          backup.test.o \
          checkpoint.test.o \
          snapshot.test.o \
          batch.test.o \
          gc.test.o \
          prefix.test.o \
          transaction-md.test.o \
          tpc.test.o \
          update.test.o \
          async.test.o \
          transaction.test.o \
          cursor.test.o \
          recover_crash.test.o \
          recover_loop.test.o \
          multithread.test.o \
          multithread_be.test.o \
          run.o

# - - - - - - - - - - - - - 

SOPHIA_DIRS     = std format runtime object version log database \
                  index \
                  transaction \
                  repository \
                  environment \
                  sophia
SOPHIA_INCLUDE  = $(addprefix -I../sophia/, $(SOPHIA_DIRS))
ifdef ENABLE_COVERAGE
STS_COVERAGE    = --coverage
else
STS_COVERAGE    =
endif
STS_CFLAGS      = $(SOPHIA_INCLUDE) -g -O0 -Wall -I. -I../sophia -Isuite/ -pthread
STS_LDFLAGS     = ../sophia.o suite/libst.a -pthread -lm $(STS_COVERAGE)
STS_BINARY      = run
STS_SOPHIA_DIR  = sophia
STS_BACKUP_DIR  = backup
STS_LOG_DIR     = log
STS_DB_DIR      = test
STS_LOG         = test.log

all: banner validate suite $(OBJECTS)
	@echo "ld $(STS_BINARY)"
	@$(CC) $(OBJECTS) $(STS_LDFLAGS) -o $(STS_BINARY)
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
	@rm -f $(STS_LOG)
	@rm -f $(OBJECTS)
cleanup: clean
	@rm -fr $(STS_SOPHIA_DIR) \
	        $(STS_BACKUP_DIR) \
	        $(STS_DB_DIR) $(STS_LOG_DIR)
test:
	@sh ./run-gdb.sh $(STS_BINARY) $(STS_LOG)

.PHONY: all clean validate banner suite test

# vim: syntax=make
