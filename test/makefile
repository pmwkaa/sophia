
# sophia test-suite
#

include ./objects.mk

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
	@rm -f $(OBJECTS)
cleanup: clean
	@rm -fr $(STS_SOPHIA_DIR) \
	        $(STS_BACKUP_DIR) \
	        $(STS_DB_DIR) $(STS_LOG_DIR)
test:
	@gdb -q -return-child-result -batch -ex "run" -ex "bt" ./$(STS_BINARY)

.PHONY: all clean validate banner suite test

# vim: syntax=make
