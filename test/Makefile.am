## Process this file with automake to produce Makefile.in

AM_CPPFLAGS	= -I$(top_srcdir)/db
AM_CFLAGS	= -Wall

check_PROGRAMS	= i common recover merge crash limit

LDADD		= ../db/libsophia.la
TESTS		= $(check_PROGRAMS)
EXTRA_DIST	= $(check_PROGRAMS)

### end of file
