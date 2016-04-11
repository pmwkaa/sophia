LIBSF_O = sf_scheme.o
LIBSF_OBJECTS = $(addprefix format/, $(LIBSF_O))
OBJECTS = $(LIBSF_O)
ifndef buildworld
  include ../mk/sophia
  include ../mk/component
endif
