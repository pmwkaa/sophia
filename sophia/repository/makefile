LIBSY_O = sy.o sy_conf.o
LIBSY_OBJECTS = $(addprefix repository/, $(LIBSY_O))
OBJECTS = $(LIBSY_O)
ifndef buildworld
  include ../mk/sophia
  include ../mk/component
endif
