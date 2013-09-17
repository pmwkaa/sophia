dnl @configure_input@
dnl

AC_PREREQ([2.69])
AC_INIT([Sophia],[0.1d0],[pmwkaa@gmail.com],
  [sophia],[https://github.com/pmwkaa/sophia/])
AC_CONFIG_SRCDIR([db/])
AC_CONFIG_MACRO_DIR([autotools])
AC_CONFIG_AUX_DIR([autotools])
AC_CANONICAL_BUILD
AC_CANONICAL_HOST
AC_CANONICAL_TARGET
AM_INIT_AUTOMAKE([foreign dist-bzip2 no-dist-gzip])
LT_PREREQ([2.4])
LT_INIT

dnl AC_LANG([C])
AC_PROG_CC_C99
AM_PROG_CC_C_O
AC_PROG_CC_STDC

AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_PROG_MKDIR_P

AC_HEADER_STDC

dnl --------------------------------------------------------------------

with_PTHREAD=check
AC_MSG_CHECKING([whether to link with pthread])
AC_ARG_WITH([pthread],
  [AS_HELP_STRING([--with-pthread],[link executable with pthread (default is check)])],
  [AS_CASE([$withval],
     [yes],  [with_PTHREAD=yes],
     [no],   [with_PTHREAD=no],
     [check],[with_PTHREAD=check],
     [AC_MSG_ERROR([bad value $withval for --with-pthread])])],
  [with_PTHREAD=check])
  AC_MSG_RESULT([$with_PTHREAD])
dnl
sophia_with_PTHREAD=no
AS_IF([test "x$with_pthread" != xno],
  [# Check for header file and shared (or static) library.
   AC_CHECK_HEADER([pthread.h],[sophia_have_pthread_h=yes],[sophia_have_pthread_h=no])
   AC_CHECK_LIB([pthread],[main],[sophia_have_pthread=yes],[sophia_have_pthread=no])
   AS_IF([test "$sophia_have_pthread" = yes && test "$sophia_have_pthread_h" = yes],
     [# Both the library and the header were found: success!
      AC_MSG_NOTICE([pthread support enabled])
      AC_DEFINE([HAVE_PTHREAD],[1],[define if you have a pthread library])
      sophia_with_PTHREAD=yes
      LIBS="$LIBS -lpthread"],

     [test "$with_pthread" = yes],
     [# The user requested support for library or die, but either
      # the header or the library or both were not found.
      AS_IF([test "$sophia_have_pthread_h" = no],
            [AC_MSG_ERROR([pthread.h cannot be found.])],
            [test "$sophia_have_pthread" = no],
            [AC_MSG_ERROR([pthread cannot be found])])],

     [# The user requested a check for optional pthread support, but
      # either the header or the library or both were not found.
      AC_MSG_WARN([pthread not found])])],
  [AC_MSG_NOTICE([pthread was not requested])])

dnl --------------------------------------------------------------------

AC_CONFIG_FILES([Makefile]
                [db/Makefile]
                [test/Makefile]
                [doc/Makefile])
AC_OUTPUT

dnl end of file
