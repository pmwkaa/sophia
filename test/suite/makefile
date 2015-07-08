
AR             = ar
RM             = rm
SOPHIA_DIRS    = std \
                 format \
                 runtime \
                 object \
                 version \
                 log \
                 database \
                 index \
                 transaction \
                 repository \
                 environment \
                 sophia
SOPHIA_INCLUDE = $(addprefix -I../../sophia/, $(SOPHIA_DIRS))
CFLAGS         = -I. -Wall -O0 -g $(SOPHIA_INCLUDE)
OBJECTS        = st_r.o st_suite.o st_generator.o st.o st_object.o st_tools.o st_scenes.o st_phase.o
TARGET         = libst.a

$(TARGET): $(OBJECTS)
	@$(AR) cr $(TARGET) $(OBJECTS)
	@echo ar $(TARGET)
.c.o:
	@$(CC) $(CFLAGS) -c $<
	@echo cc $<
clean:
	@$(RM) -f $(OBJECTS) $(TARGET)
