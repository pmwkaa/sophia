# sophia makefile.
#

CC           ?= gcc
AR            = ar
RM            = rm
LN            = ln
STRIP         = strip
VERMAJOR      = 1
VERMINOR      = 1
TARGET_STATIC = libsophia.a
TARGET_DSOLIB = libsophia.so.$(VERMAJOR).$(VERMINOR)
TARGET_DSO    = libsophia.so
PREFIX       ?= /usr/local
CFLAGS       ?= -I. -pthread -std=c99 -pedantic -Wextra -Wall -march=native -fPIC -fvisibility=hidden -O2 -DNDEBUG
LDFLAGS      ?= -shared -soname $(TARGET_DSO).$(VERMAJOR)

OBJS =  file.o \
	crc.o \
	e.o \
	i.o \
	cat.o \
	rep.o \
	util.o \
	sp.o \
	recover.o \
	merge.o \
	gc.o \
	cursor.o

ALL: $(TARGET_STATIC) $(TARGET_DSO)

$(TARGET_STATIC): clean $(OBJS)
	$(AR) crus $(TARGET_STATIC) $(OBJS)

$(TARGET_DSO): clean $(OBJS)
	$(LD) $(OBJS) $(LDFLAGS) -o $(TARGET_DSOLIB)
	$(STRIP) --strip-unneeded $(TARGET_DSOLIB)

.c.o:
	$(CC) $(CFLAGS) -c $<

install:
	mkdir -p $(PREFIX)/include
	install sophia.h $(PREFIX)/include
	mkdir -p $(PREFIX)/lib
	install $(TARGET_STATIC) $(TARGET_DSOLIB) $(PREFIX)/lib
	$(LN) -s $(PREFIX)/lib/$(TARGET_DSOLIB) $(PREFIX)/lib/$(TARGET_DSO)
	$(LN) -s $(PREFIX)/lib/$(TARGET_DSOLIB) $(PREFIX)/lib/$(TARGET_DSO).$(VERMAJOR)

clean:
	$(RM) -f $(OBJS) $(TARGET_STATIC) $(TARGET_DSOLIB)

.PHONY: install clean
