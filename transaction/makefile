LIBSX_O = sx.o sx_v.o sx_iter.o sx_deadlock.o
LIBSX_OBJECTS = $(addprefix transaction/, $(LIBSX_O))
OBJECTS = $(LIBSX_O)
ifndef buildworld
  include ../build/config
  include ../build/lib
endif
