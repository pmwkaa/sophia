#!/bin/sh

gdb -q -return-child-result -batch -ex "run" -ex "bt" --args $1 -vFr -l $2
