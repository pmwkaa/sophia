
# sophia makefile
#

CC ?= cc

ifdef ENABLE_COVERAGE
CFLAGS_COVERAGE = --coverage 
else
CFLAGS_COVERAGE =
endif

CFLAGS_DEBUG   = -DSS_INJECTION_ENABLE -g
CFLAGS_OPT     = -O2
CFLAGS_STRICT  = -std=c99 -pedantic -Wextra -Wall
CFLAGS_MISC    = -Wno-unused-function -fPIC -fno-stack-protector -fvisibility=hidden
SOPHIA_CFLAGS  = $(CFLAGS_DEBUG) \
                 $(CFLAGS_OPT) \
                 $(CFLAGS_STRICT) \
                 $(CFLAGS_COVERAGE) \
                 $(CFLAGS_MISC) \
                 $(CFLAGS)
SOPHIA_LDFLAGS = -shared $(LDFLAGS)
SOPHIA_DEP     = $(wildcard sophia/*/*) makefile
SOPHIA_VMAJOR  = 2
SOPHIA_VMINOR  = 1
SOPHIA_BUILD   = `git rev-parse --short HEAD`

all: banner static dynamic
banner:
	@echo "SOPHIA $(SOPHIA_VMAJOR).$(SOPHIA_VMINOR) (git: $(SOPHIA_BUILD))"
	@echo
sophia.c: $(SOPHIA_DEP)
	@echo sophia/build
	@sh sophia/build sophia $@
sophia.h: sophia/sophia/sophia.h
	@cp $< .
sophia.o: sophia.c sophia.h
	@echo "$(CC) sophia.c -c $(CFLAGS_DEBUG) $(CFLAGS_COVERAGE)$(CFLAGS_OPT) $(CFLAGS_STRICT)"
	@$(CC) $(SOPHIA_CFLAGS) -c sophia.c -o sophia.o
	@echo
libsophia.a: sophia.o
	@ar crs libsophia.a sophia.o
libsophia.so: sophia.o
	@$(CC) sophia.o $(SOPHIA_LDFLAGS) -o libsophia.so.2.1.1
	@ln -sf libsophia.so.2.1.1 libsophia.so.2.1
	@ln -sf libsophia.so.2.1.1 libsophia.so
ifeq ($(shell uname), Linux)
	@strip --strip-unneeded libsophia.so.2.1.1
endif
ifeq ($(shell uname), Darwin)
	@strip -u -r -x libsophia.so.2.1.1
endif
static: libsophia.a
dynamic: libsophia.so
clean:
	@rm -f sophia.c sophia.h sophia.o
	@rm -f libsophia.a
	@rm -f libsophia.so
	@rm -f libsophia.so.2.1
	@rm -f libsophia.so.2.1.1

.PHONY: all banner static dynamic clean
