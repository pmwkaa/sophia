
Build
-----

Sophia has no external dependencies.

Run *make* to generate **sophia.c** and **sophia.h** and build the library.

```
make
```

Following command can be used to compile the library in your project:

```
cc -O2 -DNDEBUG -std=c99 -pedantic -Wall -Wextra -pthread -c sophia.c
```

To build and run tests:

```
cd test
make
./sophia-test
```
