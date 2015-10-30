LIBSE_O = se.o \
          se_o.o \
          se_meta.o \
          se_metacursor.o \
          se_worker.o \
          se_req.o \
          se_v.o \
          se_db.o \
          se_tx.o \
          se_cursor.o \
          se_snapshot.o \
          se_snapshotcursor.o \
          se_execute.o \
          se_recover.o \
          se_scheduler.o
LIBSE_OBJECTS = $(addprefix environment/, $(LIBSE_O))
OBJECTS = $(LIBSE_O)
ifndef buildworld
  include ../mk/sophia
  include ../mk/component
endif
