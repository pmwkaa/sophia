
# sophia examples
#

all: validate clean crud upsert transaction cursor
validate:
	@if [ ! -f ../sophia.o ]; then \
		echo "Please build sophia first."; \
		echo ""; \
		exit 1; \
	fi
crud:
	gcc crud.c ../libsophia.a -I../ -pedantic -std=c99 -Wall -Wextra -pthread -g -o crud
upsert:
	gcc upsert.c ../libsophia.a -I../ -pedantic -std=c99 -Wall -Wextra -pthread -g -o upsert
transaction:
	gcc transaction.c ../libsophia.a -I../ -pedantic -std=c99 -Wall -Wextra -pthread -g -o transaction
cursor:
	gcc cursor.c ../libsophia.a -I../ -pedantic -std=c99 -Wall -Wextra -pthread -g -o cursor
clean:
	@rm -rf _test
	@rm -f crud
	@rm -f upsert
	@rm -f transaction
	@rm -f cursor

.PHONY: all validate clean

# vim: syntax=make
