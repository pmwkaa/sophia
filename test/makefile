LIBST_O = st.o \
          st_scene.o \
          run.o \
          srctl.test.o \
          srpager.test.o \
          sraslab.test.o \
          svindex.test.o \
          svindexiter.test.o \
          svmergeiter.test.o \
          svseaveiter.test.o \
          sl.test.o \
          sliter.test.o \
          sdbuild.test.o \
          sdv.test.o \
          sditer.test.o \
          sdpageiter.test.o \
		  sdss.test.o \
		  sdssiter.test.o \
          ctl.test.o \
          error.test.o \
          method.test.o \
          repository.test.o \
          dml.test.o \
          snapshot.test.o \
          snapshot_crash.test.o \
          branch.test.o \
          checkpoint.test.o \
          tpr.test.o \
          profiler.test.o \
          object.test.o \
          transaction.test.o \
          tpc.test.o \
          log_cursor.test.o \
          deadlock.test.o \
          cursor.test.o \
          recover_crash.test.o \
          recover_loop.test.o \
          mt.test.o \
          mt_backend.test.o
LIBST_OBJECTS = $(addprefix test-ref/, $(LIBST_O))
OBJECTS = $(LIBST_O)
include ../build/config
include ../build/test
