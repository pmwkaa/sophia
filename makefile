all:
	@(cd db; $(MAKE))
	@(cd test; $(MAKE))

install:
	@(cd db; $(MAKE) install)

clean:
	@(cd db; $(MAKE) clean)
	@(cd test; $(MAKE) clean)

.PHONY: install clean
