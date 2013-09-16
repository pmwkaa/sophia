
all:
	@(cd db; $(MAKE) --no-print-directory)
	@(cd test; $(MAKE) --no-print-directory)
clean:
	@(cd db; $(MAKE) clean --no-print-directory)
	@(cd test; $(MAKE) clean --no-print-directory)
