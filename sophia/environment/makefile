LIBSE_O = se.o \
          se_o.o \
          se_conf.o \
          se_confcursor.o \
          se_document.o \
          se_db.o \
          se_tx.o \
          se_read.o \
          se_cursor.o \
          se_recover.o
LIBSE_OBJECTS = $(addprefix environment/, $(LIBSE_O))
OBJECTS = $(LIBSE_O)
ifndef buildworld
  include ../mk/sophia
  include ../mk/component
endif
