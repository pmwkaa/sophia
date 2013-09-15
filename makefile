
all:
	@(cd db; make --no-print-directory)
	@(cd test; make --no-print-directory)
clean:
	@(cd db; make clean --no-print-directory)
	@(cd test; make clean --no-print-directory)
