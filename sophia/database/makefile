LIBSD_O = sd_v.o \
          sd_pageiter.o \
          sd_build.o \
          sd_index.o \
          sd_indexiter.o \
          sd_merge.o \
          sd_commit.o \
          sd_recover.o \
          sd_iter.o \
          sd_scheme.o \
          sd_schemeiter.o
LIBSD_OBJECTS = $(addprefix database/, $(LIBSD_O))
OBJECTS = $(LIBSD_O)
ifndef buildworld
  include ../mk/sophia
  include ../mk/component
endif
