LIBSE_O = se.o \
          se_o.o \
          se_meta.o \
          se_metacursor.o \
          se_worker.o \
          se_request.o \
          se_query.o \
          se_v.o \
          se_db.o \
          se_tx.o \
          se_cursor.o \
          se_snapshot.o \
          se_recover.o \
          se_scheduler.o
LIBSE_OBJECTS = $(addprefix environment/, $(LIBSE_O))
OBJECTS = $(LIBSE_O)
ifndef buildworld
  include ../mk/sophia
  include ../mk/component
endif
