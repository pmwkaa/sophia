
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libst.h>

static void
srzstdfilter_compress_decompress(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	sr r;
	sr_init(&r, NULL, &a, NULL, SR_FKV, NULL, NULL, NULL, NULL);

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

	srbuf compressed;
	sr_bufinit(&compressed);

	srfilter f;
	t( sr_filterinit(&f, &sr_zstdfilter, &r, SR_FINPUT) == 0 );
	t( sr_filterstart(&f, &compressed) == 0 );
	t( sr_filternext(&f, &compressed, text, sizeof(text) - 1) == 0 );
	t( sr_filtercomplete(&f, &compressed) == 0 );
	t( sr_filterfree(&f) == 0 );

	srbuf decompressed;
	sr_bufinit(&decompressed);
	t( sr_bufensure(&decompressed, &a, sizeof(text)) == 0 );

	t( sr_filterinit(&f, &sr_zstdfilter, &r, SR_FOUTPUT) == 0 );
	t( sr_filternext(&f, &decompressed, compressed.s, sr_bufused(&compressed)) == 0 );
	t( sr_filterfree(&f) == 0 );

	t( memcmp(text, decompressed.s, sizeof(text) - 1) == 0 );

	sr_buffree(&compressed, &a);
	sr_buffree(&decompressed, &a);
}

stgroup *srzstdfilter_group(void)
{
	stgroup *group = st_group("srzstdfilter");
	st_groupadd(group, st_test("compress_decompress", srzstdfilter_compress_decompress));
	return group;
}
