
CC      ?= gcc
RM      ?= rm
CFLAGS  ?= -I. -I../db -Wall -pthread -O0 -g
LDFLAGS ?= -pthread ../db/libsophia.a
all: clean i common recover merge concurrent transaction crash limit issues
common: common.o
	$(CC) common.o $(LDFLAGS) -o common
recover: recover.o
	$(CC) recover.o $(LDFLAGS) -o recover
merge: merge.o
	$(CC) merge.o $(LDFLAGS) -o merge
crash: crash.o
	$(CC) crash.o $(LDFLAGS) -o crash
concurrent: concurrent.o
	$(CC) concurrent.o $(LDFLAGS) -o concurrent
transaction: transaction.o
	$(CC) transaction.o $(LDFLAGS) -o transaction
limit: limit.o
	$(CC) limit.o $(LDFLAGS) -o limit
issues: issues.o
	$(CC) issues.o $(LDFLAGS) -o issues
i: i.o
	$(CC) i.o $(LDFLAGS) -o i
.c.o:
	$(CC) $(CFLAGS) -c $<
clean:
	$(RM) -f common.o common recover.o recover merge.o merge
	$(RM) -f crash.o crash i.o i concurrent.o concurrent
	$(RM) -f transaction.o transaction limit.o limit issues.o issues
test:
	$(VALGRIND) ./i
	$(VALGRIND) ./common
	$(VALGRIND) ./recover
	$(VALGRIND) ./merge
	$(VALGRIND) ./crash
	$(VALGRIND) ./transaction
	$(VALGRIND) ./limit
	$(VALGRIND) ./issues
	./concurrent
