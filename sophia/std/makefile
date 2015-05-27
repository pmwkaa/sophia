LIBSS_O = ss_time.o \
          ss_trigger.o \
          ss_pager.o \
          ss_stda.o \
          ss_slaba.o \
          ss_quota.o \
          ss_rb.o \
          ss_bufiter.o \
          ss_thread.o \
          ss_file.o \
          ss_dir.o \
          ss_map.o \
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
