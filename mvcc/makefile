LIBSM_O = sm.o sm_deadlock.o sm_iter.o
LIBSM_OBJECTS = $(addprefix mvcc/, $(LIBSM_O))
OBJECTS = $(LIBSM_O)
ifndef buildworld
  include ../build/config
  include ../build/lib
endif
