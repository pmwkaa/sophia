LIBSI_O = si_scheme.o \
          si_node.o \
          si_planner.o \
          si.o \
          si_gc.o \
          si_read.o \
          si_write.o \
          si_query.o \
          si_iter.o \
          si_balance.o \
          si_compaction.o \
          si_drop.o \
          si_backup.o \
          si_profiler.o \
          si_recover.o
LIBSI_OBJECTS = $(addprefix index/, $(LIBSI_O))
OBJECTS = $(LIBSI_O)
ifndef buildworld
  include ../mk/sophia
  include ../mk/component
endif
