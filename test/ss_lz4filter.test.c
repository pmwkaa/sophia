
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
ss_lz4filter_compress_decompress(void)
{
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
	t( ss_filterinit(&f, &ss_lz4filter, &st_r.a, SS_FINPUT) == 0 );
	t( ss_filterstart(&f, &compressed) == 0 );
	t( ss_filternext(&f, &compressed, text, sizeof(text) - 1) == 0 );
	t( ss_filtercomplete(&f, &compressed) == 0 );
	t( ss_filterfree(&f) == 0 );

	ssbuf decompressed;
	ss_bufinit(&decompressed);
	t( ss_bufensure(&decompressed, &st_r.a, sizeof(text)) == 0 );

	t( ss_filterinit(&f, &ss_lz4filter, &st_r.a, SS_FOUTPUT) == 0 );
	t( ss_filternext(&f, &decompressed, compressed.s, ss_bufused(&compressed)) == 0 );
	t( ss_filterfree(&f) == 0 );

	t( memcmp(text, decompressed.s, sizeof(text) - 1) == 0 );

	ss_buffree(&compressed, &st_r.a);
	ss_buffree(&decompressed, &st_r.a);
}

stgroup *ss_lz4filter_group(void)
{
	stgroup *group = st_group("sslz4filter");
	st_groupadd(group, st_test("compress_decompress", ss_lz4filter_compress_decompress));
	return group;
}
