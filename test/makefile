LIBST_O = st.o \
          st_scene.o \
          run.o \
          sl.test.o \
          sliter.test.o \
          sdbuild.test.o \
          sdv.test.o \
          sditer.test.o \
          sdpageiter.test.o \
          svindex.test.o \
          svindexiter.test.o \
          svmergeiter.test.o \
          svseaveiter.test.o \
          ctl.test.o \
          dml.test.o \
          profiler.test.o \
          object.test.o \
          transaction.test.o \
          cursor.test.o \
          recover_crash.test.o \
          recover_loop.test.o \
          multithread.test.o
LIBST_OBJECTS = $(addprefix test-ref/, $(LIBST_O))
OBJECTS = $(LIBST_O)
include ../build/config
include ../build/test
