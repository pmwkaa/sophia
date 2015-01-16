LIBSR_O = sr_crc.o \
          sr_time.o \
          sr_trigger.o \
          sr_cmp.o \
          sr_pager.o \
          sr_astd.o \
          sr_aslab.o \
          sr_quota.o \
          sr_rb.o \
          sr_bufiter.o \
          sr_thread.o \
          sr_c.o \
          sr_file.o \
          sr_dir.o \
          sr_map.o
LIBSR_OBJECTS = $(addprefix rt/, $(LIBSR_O))
OBJECTS = $(LIBSR_O)
ifndef buildworld
  include ../build/config
  include ../build/lib
endif
