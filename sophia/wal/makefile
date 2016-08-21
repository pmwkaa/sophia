LIBSW_O = sw_dir.o sw.o sw_conf.o sw_iter.o
LIBSW_OBJECTS = $(addprefix wal/, $(LIBSW_O))
OBJECTS = $(LIBSW_O)
ifndef buildworld
  include ../mk/sophia
  include ../mk/component
endif
