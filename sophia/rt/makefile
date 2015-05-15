LIBSR_O = sr_time.o \
          sr_scheme.o \
          sr_trigger.o \
          sr_pager.o \
          sr_stda.o \
          sr_slaba.o \
          sr_quota.o \
          sr_rb.o \
          sr_bufiter.o \
          sr_thread.o \
          sr_c.o \
          sr_file.o \
          sr_dir.o \
          sr_map.o \
          sr_crc.o \
          sr_lz4filter.o \
          sr_zstdfilter.o
LIBSR_OBJECTS = $(addprefix rt/, $(LIBSR_O))
OBJECTS = $(LIBSR_O)
ifndef buildworld
  include ../mk/sophia
  include ../mk/component
endif
