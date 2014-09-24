LIBSD_O = sd_v.o \
          sd_pageiter.o \
          sd_build.o \
          sd_index.o \
          sd_indexiter.o \
          sd_merge.o \
          sd_iter.o
LIBSD_OBJECTS = $(addprefix database/, $(LIBSD_O))
OBJECTS = $(LIBSD_O)
ifndef buildworld
  include ../build/config
  include ../build/lib
endif
