#!/bin/sh

# sophia amalgamation build
#

process() {
	root=$1
	output=$2
	lib=$3
	dir=$4
	includes=`cat $root/$dir/$lib | sed -n 's/#include <\(.*\)>/\1/p'`
	for file in $includes; do
		cat "$root/$dir/$file" >> $output
	done
	files=`ls $root/$dir/*.c`
	for file in $files; do
		cat "$file" | grep -v "include" >> $output
	done
}

if [ $# -ne 2 ]; then
	echo "sophia amalgamation build."
	echo "usage: $0 <sophia_root> <output>"
	return 1
fi

root=$1
output=$2

rm -f $output
touch $output

process $root $output "libsr.h" "rt"
process $root $output "libsv.h" "version"
process $root $output "libsm.h" "mvcc"
process $root $output "libsl.h" "log"
process $root $output "libsd.h" "database"
process $root $output "libsi.h" "index"
process $root $output "libse.h" "repository"
process $root $output "libso.h" "sophia"
