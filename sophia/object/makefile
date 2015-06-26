LIBSO_O = so.o
LIBSO_OBJECTS = $(addprefix object/, $(LIBSO_O))
OBJECTS = $(LIBSO_O)
ifndef buildworld
  include ../mk/sophia
  include ../mk/component
endif
