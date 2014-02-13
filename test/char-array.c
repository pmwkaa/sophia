
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <string.h>
#include <sophia.h>
#include "test.h"

static char *dbrep = "./rep";

static void
char_arrays(void)
{
  void *env = sp_env();
  t( env != NULL );
  t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
  void *db = sp_open(env);
  t( db != NULL );

  char key1[] = "Lorem ipsum 1";
  char value1[] = "Culpa anim anim eiusmod minim.";
  t( sp_set(db, key1, strlen(key1), value1, strlen(value1)) == 0);

  char key2[] = "Lorem ipsum 10";
  char value2[] = "Velit aliquip pariatur occaecat officia.";
  t( sp_set(db, key2, strlen(key2), value2, strlen(value2)) == 0);

  void *res = NULL;
  size_t ressize = 0;

  t( sp_get(db, key1, strlen(key1), &res, &ressize) == 1 );
  t( strcmp(value1, res) == 0);

  free(res);

  t( sp_destroy(db) == 0 );
  t( sp_destroy(env) == 0 );
  t( rmrf(dbrep) == 0 );
}

int
main(int argc, char *argv[])
{
  rmrf(dbrep);

  test(char_arrays);
  return 0;
}
