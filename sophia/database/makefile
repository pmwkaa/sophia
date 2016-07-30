LIBSD_O = sd_pageiter.o \
          sd_build.o \
          sd_buildindex.o \
          sd_indexiter.o \
          sd_merge.o \
          sd_read.o \
          sd_write.o \
          sd_io.o \
          sd_iter.o \
          sd_scheme.o \
          sd_schemeiter.o \
          sd_snapshot.o \
          sd_snapshotiter.o
LIBSD_OBJECTS = $(addprefix database/, $(LIBSD_O))
OBJECTS = $(LIBSD_O)
ifndef buildworld
  include ../mk/sophia
  include ../mk/component
endif
