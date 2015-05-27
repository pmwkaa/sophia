
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libst.h>

static void
srscheme_saveload(stc *cx)
{
	ssa a;
	ss_aopen(&a, &ss_stda);

	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );

	ssbuf buf;
	ss_bufinit(&buf);
	t( sr_schemesave(&cmp, &a, &buf) == 0 );
	sr_schemefree(&cmp, &a);

	sr_schemeinit(&cmp);
	t( sr_schemeload(&cmp, &a, buf.s, ss_bufused(&buf)) == 0 );

	t( cmp.count == 1 );
	t( strcmp(cmp.parts[0].name, "key") == 0 );
	t( cmp.parts[0].type == SS_U32 );

	sr_schemefree(&cmp, &a);
	ss_buffree(&buf, &a);
}

stgroup *srscheme_group(void)
{
	stgroup *group = st_group("srscheme");
	st_groupadd(group, st_test("save_load", srscheme_saveload));
	return group;
}
