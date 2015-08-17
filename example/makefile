
# sophia examples
#

all: validate clean set-get-delete transaction batch cursor snapshot
validate:
	@if [ ! -f ../sophia.o ]; then \
		echo "Please build sophia first."; \
		echo ""; \
		exit 1; \
	fi
set-get-delete:
	gcc crud.c ../libsophia.a -I../ -pedantic -std=c99 -Wall -Wextra -pthread -g -o crud
transaction:
	gcc transaction.c ../libsophia.a -I../ -pedantic -std=c99 -Wall -Wextra -pthread -g -o transaction
batch:
	gcc batch.c ../libsophia.a -I../ -pedantic -std=c99 -Wall -Wextra -pthread -g -o batch
cursor:
	gcc cursor.c ../libsophia.a -I../ -pedantic -std=c99 -Wall -Wextra -pthread -g -o cursor
snapshot:
	gcc snapshot.c ../libsophia.a -I../ -pedantic -std=c99 -Wall -Wextra -pthread -g -o snapshot
clean:
	@rm -rf _test
	@rm -f crud
	@rm -f transaction
	@rm -f batch
	@rm -f cursor
	@rm -f snapshot

.PHONY: all validate clean

# vim: syntax=make
