LIBSV_O = sv_v.o \
          sv_index.o \
          sv_indexiter.o \
          sv_mergeiter.o \
          sv_readiter.o \
          sv_writeiter.o
LIBSV_OBJECTS = $(addprefix version/, $(LIBSV_O))
OBJECTS = $(LIBSV_O)
ifndef buildworld
  include ../mk/sophia
  include ../mk/component
endif
