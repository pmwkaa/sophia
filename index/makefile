LIBSI_O = si_node.o \
          si_planner.o \
          si.o \
          si_commit.o \
          si_query.o \
          si_iter.o \
          si_split.o \
		  si_balance.o \
          si_compaction.o \
          si_profiler.o \
          si_recover.o
LIBSI_OBJECTS = $(addprefix index/, $(LIBSI_O))
OBJECTS = $(LIBSI_O)
ifndef buildworld
  include ../build/config
  include ../build/lib
endif
