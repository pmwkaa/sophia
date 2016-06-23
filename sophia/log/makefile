LIBSL_O = sl_dir.o sl.o sl_conf.o sl_iter.o
LIBSL_OBJECTS = $(addprefix log/, $(LIBSL_O))
OBJECTS = $(LIBSL_O)
ifndef buildworld
  include ../mk/sophia
  include ../mk/component
endif
