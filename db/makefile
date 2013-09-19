
#
# sophia makefile.
#

CC           ?= gcc
RM           ?= rm
LN           ?= ln
VERMAJOR      = 1
VERMINOR      = 1
TARGET_STATIC = libsophia.a
TARGET_DSOLIB = libsophia.so.$(VERMAJOR).$(VERMINOR)
TARGET_DSO    = libsophia.so
CFLAGS       ?= -I. -std=c99 -pedantic -Wextra -Wall -pthread -O2 -DNDEBUG -fPIC
LDFLAGS      ?= -shared -soname libsophia.$(VERMAJOR)

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
	$(AR) cru $(TARGET_STATIC) $(OBJS)

$(TARGET_DSO): clean $(OBJS)
	$(LD) $(OBJS) $(LDFLAGS) -o $(TARGET_DSOLIB)
	$(LN) -s $(TARGET_DSOLIB) $(TARGET_DSO).$(VERMAJOR)
	$(LN) -s $(TARGET_DSOLIB) $(TARGET_DSO)

.c.o:
	$(CC) $(CFLAGS) -c $<

clean:
	$(RM) -f $(OBJS) $(TARGET_STATIC)
	$(RM) -f $(TARGET_DSOLIB) $(TARGET_DSO).$(VERMAJOR) $(TARGET_DSO)
