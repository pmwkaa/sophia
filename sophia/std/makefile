LIBSS_O = ss_time.o \
          ss_ooma.o \
          ss_stda.o \
          ss_slaba.o \
          ss_rb.o \
          ss_bufiter.o \
          ss_thread.o \
          ss_stdvfs.o \
          ss_testvfs.o \
          ss_crc.o \
          ss_nonefilter.o \
          ss_lz4filter.o \
          ss_zstdfilter.o
LIBSS_OBJECTS = $(addprefix std/, $(LIBSS_O))
OBJECTS = $(LIBSS_O)
ifndef buildworld
  include ../mk/sophia
  include ../mk/component
endif
