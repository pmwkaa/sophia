
CC      ?= gcc
RM      ?= rm
CFLAGS  ?= -I. -std=c99 -pedantic -Wextra -Wall -pthread -O2 -DNDEBUG -fPIC
LDFLAGS ?= -shared -soname $(SONAME) -o libsophia.so.1.0
OBJECTS = file.o \
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
LIBRARIES = -lc\
	    -lpthread

ALL: $(TARGET) so

TARGET  = libsophia.a
SONAME = libsophia.so.1
TARGET_SO = libsophia.so.1.0

$(TARGET): clean $(OBJECTS)
	$(AR) cru $(TARGET) $(OBJECTS)

so: clean $(OBJECTS)
	$(LD) $(LDFLAGS) $(OBJECTS) $(LIBRARIES)

.c.o:
	$(CC) $(CFLAGS) -c $<
clean:
	$(RM) -f $(OBJECTS) $(TARGET)
