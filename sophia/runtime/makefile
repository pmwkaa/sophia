LIBSR_O = sr_scheme.o sr_meta.o
LIBSR_OBJECTS = $(addprefix runtime/, $(LIBSR_O))
OBJECTS = $(LIBSR_O)
ifndef buildworld
  include ../mk/sophia
  include ../mk/component
endif
