LIBSX_O = sx.o sx_v.o sx_deadlock.o
LIBSX_OBJECTS = $(addprefix transaction/, $(LIBSX_O))
OBJECTS = $(LIBSX_O)
ifndef buildworld
  include ../mk/sophia
  include ../mk/component
endif
