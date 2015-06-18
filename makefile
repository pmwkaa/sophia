
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

all: static dynamic
banner:
	@echo SOPHIA v1.2.3
	@echo "cc: $(CC)"
	@echo "cflags: $(CFLAGS_DEBUG) $(CFLAGS_COVERAGE)$(CFLAGS_OPT) $(CFLAGS_STRICT)"
	@echo
sophia.c:
	@echo build
	@sh sophia/build sophia $@
	@echo cc $@
sophia.h:
	@cp sophia/sophia/sophia.h .
sophia.o: banner clean sophia.c sophia.h
	@$(CC) $(SOPHIA_CFLAGS) -c sophia.c -o sophia.o
libsophia.a: sophia.o
	@echo "ar libsophia.a"
	@ar crs libsophia.a sophia.o
libsophia.so: sophia.o
	@echo "ld libsophia.so"
	@$(CC) sophia.o $(SOPHIA_LDFLAGS) -o libsophia.so.1.2.3
	@ln -sf libsophia.so.1.2.3 libsophia.so.1.2
	@ln -sf libsophia.so.1.2.3 libsophia.so
ifeq ($(shell uname), Linux)
	@strip --strip-unneeded libsophia.so.1.2.3
endif
ifeq ($(shell uname), Darwin)
	@strip -u -r -x libsophia.so.1.2.3
endif
static: libsophia.a
dynamic: libsophia.so
clean:
	@rm -f sophia.c sophia.h sophia.o
	@rm -f libsophia.a
	@rm -f libsophia.so
	@rm -f libsophia.so.1.2
	@rm -f libsophia.so.1.2.3

.PHONY: all banner static dynamic clean
