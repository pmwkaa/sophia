
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
sslz4filter_compress_decompress(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	sr r;
	sr_init(&r, NULL, &a, NULL, SF_KV, SF_SRAW, NULL, NULL, NULL, NULL);

	char text[] =
	"The Early English Text Society is a text publication society dedicated to the"
	"reprinting of early English texts, especially those only available in manuscript."
	"Most of its volumes are in Middle English and Old English. It was founded in "
	"England in 1864 by Frederick James Furnivall. Its stated goal in a report of the "
	"first year of their existence was \"on the one hand, to print all that is most "
	"valuable of the yet unprinted MSS. in English, and, on the other, to re-edit "
	"and reprint all that is most valuable in printed English books, which from their "
	"scarcity or price are not within the reach of the student of moderate means.\"[1]"
	"It is known for being the first to print many English "
	"manuscripts, including Cotton Nero A.x, which contains Pearl, Sir Gawain and the "
	"Green Knight, and other poems. By its own count, the Society has published 344"
	"volumes. Famous members of the society when it was formed in 1864 include "
	"Furnivall himself, Alfred Tennyson (poet laureate), Warren de la Rue (inventor "
	"of the lightbulb), Richard Chenevix Trench (Irish ecclesiastic), Stephen Austin "
	"(a Hertford-based printer), Edith Coleridge (granddaughter of Samuel Taylor "
	"Coleridge), and others.";

	ssbuf compressed;
	ss_bufinit(&compressed);

	ssfilter f;
	t( ss_filterinit(&f, &ss_lz4filter, &a, SS_FINPUT) == 0 );
	t( ss_filterstart(&f, &compressed) == 0 );
	t( ss_filternext(&f, &compressed, text, sizeof(text) - 1) == 0 );
	t( ss_filtercomplete(&f, &compressed) == 0 );
	t( ss_filterfree(&f) == 0 );

	ssbuf decompressed;
	ss_bufinit(&decompressed);
	t( ss_bufensure(&decompressed, &a, sizeof(text)) == 0 );

	t( ss_filterinit(&f, &ss_lz4filter, &a, SS_FOUTPUT) == 0 );
	t( ss_filternext(&f, &decompressed, compressed.s, ss_bufused(&compressed)) == 0 );
	t( ss_filterfree(&f) == 0 );

	t( memcmp(text, decompressed.s, sizeof(text) - 1) == 0 );

	ss_buffree(&compressed, &a);
	ss_buffree(&decompressed, &a);
}

stgroup *sslz4filter_group(void)
{
	stgroup *group = st_group("sslz4filter");
	st_groupadd(group, st_test("compress_decompress", sslz4filter_compress_decompress));
	return group;
}
