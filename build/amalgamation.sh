#!/bin/sh

process() {
	output=$1
	dir=$2
	lib=$3
	includes=`cat $dir/$lib | sed -n 's/#include <\(.*\)>/\1/p'`
	for file in $includes; do
		cat "$dir/$file" >> $output
	done
	files=`ls $dir/*.c`
	for file in $files; do
		cat "$file" | grep -v "include" >> $output
	done
}

output="sophia.c"
rm -f $output
touch $output

process $output "rt" "libsr.h"
process $output "version" "libsv.h"
process $output "mvcc" "libsm.h"
process $output "log" "libsl.h"
process $output "database" "libsd.h"
process $output "index" "libsi.h"
process $output "repository" "libse.h"
process $output "sophia" "libso.h"
