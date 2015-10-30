
# sophia examples
#

all: validate clean set-get-delete transaction cursor
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
cursor:
	gcc cursor.c ../libsophia.a -I../ -pedantic -std=c99 -Wall -Wextra -pthread -g -o cursor
clean:
	@rm -rf _test
	@rm -f crud
	@rm -f transaction
	@rm -f cursor

.PHONY: all validate clean

# vim: syntax=make
