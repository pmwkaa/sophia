
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
sf_scheme_saveload(void)
{
	sfscheme cmp;
	sf_schemeinit(&cmp);

	sffield *field;
	field = sf_fieldnew(&st_r.a, "key");
	t( field != NULL );
	t( sf_fieldoptions(field, &st_r.a, "u32,key(0)") == 0);
	t( sf_schemeadd(&cmp, &st_r.a, field) == 0);

	field = sf_fieldnew(&st_r.a, "value");
	t( field != NULL );
	t( sf_fieldoptions(field, &st_r.a, "string") == 0);
	t( sf_schemeadd(&cmp, &st_r.a, field) == 0);
	t( sf_schemevalidate(&cmp, &st_r.a) == 0 );

	ssbuf buf;
	ss_bufinit(&buf);
	t( sf_schemesave(&cmp, &st_r.a, &buf) == 0 );
	sf_schemefree(&cmp, &st_r.a);

	sf_schemeinit(&cmp);
	t( sf_schemeload(&cmp, &st_r.a, buf.s, ss_bufused(&buf)) == 0 );
	t( sf_schemevalidate(&cmp, &st_r.a) == 0 );

	t( cmp.fields_count == 2 + 1 + 1);
	t( cmp.keys_count == 1 );
	t( strcmp(cmp.fields[0]->name, "key") == 0 );
	t( cmp.fields[0]->type == SS_U32 );
	t( cmp.fields[0]->key == 1 );
	t( cmp.fields[1]->type == SS_STRING );
	t( cmp.fields[1]->key == 0 );

	t( cmp.fields[2]->type == SS_U8 );
	t( cmp.fields[2]->key == 0 );
	t( cmp.fields[2]->flags == 1 );

	t( cmp.fields[3]->type == SS_U64 );
	t( cmp.fields[3]->key == 0 );
	t( cmp.fields[3]->lsn == 1 );

	sf_schemefree(&cmp, &st_r.a);
	ss_buffree(&buf, &st_r.a);
}

stgroup *sf_scheme_group(void)
{
	stgroup *group = st_group("sfscheme");
	st_groupadd(group, st_test("save_load", sf_scheme_saveload));
	return group;
}
