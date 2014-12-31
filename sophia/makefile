SOPHIA_O = so_ctl.o \
           so_ctlcursor.o \
           so_worker.o \
           so_scheduler.o \
           so.o \
           so_v.o \
           so_db.o \
           so_tx.o \
           so_logcursor.o \
           so_cursor.o \
           so_snapshot.o \
           so_recover.o \
           sophia.o
SOPHIA_OBJECTS = $(addprefix sophia/, $(SOPHIA_O))
OBJECTS = $(SOPHIA_O)
ifndef buildworld
  include ../build/config
  include ../build/lib
endif
