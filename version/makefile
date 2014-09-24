LIBSV_O = sv_local.o \
          sv_cmpiter.o \
          sv_mergeiter.o \
          sv_seaveiter.o \
          sv_v.o \
          sv_index.o \
          sv_indexiter.o
LIBSV_OBJECTS = $(addprefix version/, $(LIBSV_O))
OBJECTS = $(LIBSV_O)
ifndef buildworld
  include ../build/config
  include ../build/lib
endif
