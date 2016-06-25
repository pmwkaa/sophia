
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <sophia.h>
#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libsv.h>
#include <libso.h>
#include <libst.h>

static void
ssslaba_test0(void)
{
	ssa a;
	ss_aopen(&a, &ss_slaba, &st_r.vfs, 1024, 16);

	void *chunks[64];
	int   chunks_size = 64;

	int iteration = 0;
	while (iteration < 8)
	{
		int i = 0;
		while ( i < chunks_size) {
			chunks[i] = ss_malloc(&a, 16);
			t( chunks[i] != NULL );
			i++;
		}
		t(i == 64);
		t( ss_malloc(&a, 16) == NULL );
		i = 0;
		while ( i < chunks_size) {
			ss_free(&a, chunks[i]);
			t( chunks[i] != NULL );
			i++;
		}
		iteration++;
	}
	ss_aclose(&a);
}

stgroup *ss_slaba_group(void)
{
	stgroup *group = st_group("ssslaba");
	st_groupadd(group, st_test("test0", ssslaba_test0));
	return group;
}
