# configure.sh --
#
# Run this to configure.

set -xe

prefix=/usr/local

../configure \
    --config-cache                              \
    --cache-file=../config.cache                \
    --prefix="${prefix}"                        \
    --disable-static --enable-shared            \
    CFLAGS='-O3'				\
    "$@"

### end of file
