
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
sr_scheme_saveload(void)
{
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp);
	t( sr_keysetname(part, &st_r.a, "key") == 0 );
	t( sr_keyset(part, &st_r.a, "u32") == 0 );

	ssbuf buf;
	ss_bufinit(&buf);
	t( sr_schemesave(&cmp, &st_r.a, &buf) == 0 );
	sr_schemefree(&cmp, &st_r.a);

	sr_schemeinit(&cmp);
	t( sr_schemeload(&cmp, &st_r.a, buf.s, ss_bufused(&buf)) == 0 );

	t( cmp.count == 1 );
	t( strcmp(cmp.parts[0].name, "key") == 0 );
	t( cmp.parts[0].type == SS_U32 );

	sr_schemefree(&cmp, &st_r.a);
	ss_buffree(&buf, &st_r.a);
}

stgroup *sr_scheme_group(void)
{
	stgroup *group = st_group("srscheme");
	st_groupadd(group, st_test("save_load", sr_scheme_saveload));
	return group;
}
