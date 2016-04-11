LIBSR_O = sr_conf.o
LIBSR_OBJECTS = $(addprefix runtime/, $(LIBSR_O))
OBJECTS = $(LIBSR_O)
ifndef buildworld
  include ../mk/sophia
  include ../mk/component
endif
