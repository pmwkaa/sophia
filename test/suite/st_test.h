#ifndef ST_TEST_H_
#define ST_TEST_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sttest sttest;

typedef void (*sttf)(void);

struct sttest {
	char *name;
	sttf function;
	sslist link;
};

static inline sttest*
st_test(char *name, sttf function)
{
	sttest *test = malloc(sizeof(*test));
	if (ssunlikely(test == NULL))
		return NULL;
	test->name = name;
	test->function = function;
	ss_listinit(&test->link);
	return test;
}

static inline void
st_testfree(sttest *test)
{
	free(test);
}

#endif
