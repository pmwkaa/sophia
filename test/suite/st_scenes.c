
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

void st_scene_rmrf(stscene *s ssunused)
{
	rmrf(st_r.conf->sophia_dir);
	rmrf(st_r.conf->backup_dir);
	rmrf(st_r.conf->log_dir);
	rmrf(st_r.conf->db_dir);
}

void st_scene_test(stscene *s ssunused)
{
	if (st_r.verbose) {
		fprintf(st_r.output, ".test");
		fflush(st_r.output);
	}
	st_r.test->function();
	st_r.stat_test++;

	if (st_r.conf->report) {
		int percent = (st_r.stat_test * 100.0) / st_r.suite.total;
		if (percent == st_r.report)
			return;
		st_r.report = percent;
		fprintf(stdout, "complete %02d%% (%d tests out of %d)\n",
		        percent,
		        st_r.stat_test,
		        st_r.suite.total);
	}
}

void st_scene_pass(stscene *s ssunused)
{
	fprintf(st_r.output,  ": ok\n");
	fflush(st_r.output);
}

void st_scene_init(stscene *s ssunused)
{
	st_listinit(&st_r.gc, ST_SVV);
	ss_aopen(&st_r.a, &ss_stda);
	ss_vfsinit(&st_r.vfs, &ss_stdvfs);
	sf_schemeinit(&st_r.scheme);
	memset(&st_r.injection, 0, sizeof(st_r.injection));
	memset(&st_r.stat, 0, sizeof(st_r.stat));
	sr_statusinit(&st_r.status);
	sr_loginit(&st_r.log);
	sr_errorinit(&st_r.error, &st_r.log);
	sr_seqinit(&st_r.seq);
	st_r.crc = ss_crc32c_function();
	st_r.fmt_storage = SF_RAW;
	st_r.compression = NULL;
	memset(&st_r.r, 0, sizeof(st_r.r));
	st_r.key_start = 8;
	st_r.key_end = 32;
	st_r.value_start = 4;
	st_r.value_end = 4;
}

void st_scene_scheme_u32(stscene *s ssunused)
{
	sffield *field = sf_fieldnew(&st_r.a, "key");
	t( field != NULL );
	t( sf_fieldoptions(field, &st_r.a, "u32,key(0)") == 0 );
	t( sf_schemeadd(&st_r.scheme, &st_r.a, field) == 0 );

	field = sf_fieldnew(&st_r.a, "value");
	t( field != NULL );
	t( sf_fieldoptions(field, &st_r.a, "string") == 0 );
	t( sf_schemeadd(&st_r.scheme, &st_r.a, field) == 0 );
}

void st_scene_rt(stscene *s ssunused)
{
	sf_schemevalidate(&st_r.scheme, &st_r.a);

	sr_init(&st_r.r, &st_r.status,
	        &st_r.log,
	        &st_r.error,
	        &st_r.a,
	        &st_r.vfs,
	        NULL, /* quota */
	        &st_r.seq,
	         st_r.fmt_storage,
	         NULL, /* update */
	        &st_r.scheme,
	        &st_r.injection,
	        &st_r.stat,
	         st_r.crc);

	st_generator_init(&st_r.g, &st_r.r,
	                  st_r.key_start,
	                  st_r.key_end,
	                  st_r.value_start,
	                  st_r.value_end);
}

void st_scene_gc(stscene *s ssunused)
{
	st_listfree(&st_r.gc, &st_r.r);
	ss_aclose(&st_r.a);
	ss_vfsfree(&st_r.vfs);
	sr_errorfree(&st_r.error);
	sr_statusfree(&st_r.status);
	sr_seqfree(&st_r.seq);
	sf_schemefree(&st_r.scheme, &st_r.a);
}

void st_scene_env(stscene *s ssunused)
{
	if (st_r.verbose) {
		fprintf(st_r.output, ".env");
		fflush(st_r.output);
	}

	t( st_r.env == NULL );
	t( st_r.db  == NULL );

	void *env = sp_env();
	t( env != NULL );
	st_r.env = env;

	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "log.enable", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.test.mmap", 0) == 0 );
	t( sp_setint(env, "db.test.compaction.page_checksum", 1) == 0 );
	t( sp_setstring(env, "db.test.compression_cold", "none", 0) == 0 );
	t( sp_setstring(env, "db.test.compression_hot", "none", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.value", "string", 0) == 0 );

	st_r.db = sp_getobject(env, "db.test");
	t( st_r.db != NULL );
}

void st_scene_branch_wm_1(stscene *s ssunused)
{
	t( sp_setint(st_r.env, "db.test.compaction.branch_wm", 1) == 0 );
}

void st_scene_thread_5(stscene *s ssunused)
{
	if (st_r.verbose) {
		fprintf(st_r.output, ".thread_5");
		fflush(st_r.output);
	}
	t( sp_setint(st_r.env, "scheduler.threads", 5) == 0 );
}

void st_scene_open(stscene *s ssunused)
{
	if (st_r.verbose) {
		fprintf(st_r.output, ".open");
		fflush(st_r.output);
	}
	t( sp_open(st_r.env) == 0 );
}

void st_scene_destroy(stscene *s ssunused)
{
	if (st_r.verbose) {
		fprintf(st_r.output, ".destroy");
		fflush(st_r.output);
	}
	t( st_r.env != NULL );
	t( sp_destroy(st_r.env) == 0 );
	st_r.env = NULL;
	st_r.db  = NULL;
}

void st_scene_truncate(stscene *s ssunused)
{
	if (st_r.verbose) {
		fprintf(st_r.output, ".truncate");
		fflush(st_r.output);
	}
	void *c = sp_cursor(st_r.env);
	t( c != NULL );
	void *o = sp_document(st_r.db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	while ((o = sp_get(c, o))) {
		void *k = sp_document(st_r.db);
		t( k != NULL );
		int i = 0;
		while (i < st_r.r.scheme->fields_count) {
			int size;
			void *field = sp_getstring(o, st_r.r.scheme->fields[i]->name, &size);
			sp_setstring(k, st_r.r.scheme->fields[i]->name, field, size);
			i++;
		}
		t( sp_delete(st_r.db, k) == 0 );
	}
	t( sp_destroy(c) == 0 );
}

void st_scene_recover(stscene *s ssunused)
{
	fprintf(st_r.output, "\n (recover) ");
	fflush(st_r.output);
}

void st_scene_phase_compaction(stscene *s)
{
	st_r.phase_compaction_scene = s->state;
	st_r.phase_compaction = 0;
	if (st_r.verbose == 0)
		return;
	switch (s->state) {
	case 0:
		fprintf(st_r.output, ".in_memory");
		fflush(st_r.output);
		break;
	case 1:
		fprintf(st_r.output, ".branch");
		fflush(st_r.output);
		break;
	case 2:
		fprintf(st_r.output, ".compact");
		fflush(st_r.output);
		break;
	case 3:
		fprintf(st_r.output, ".logrotate_gc");
		fflush(st_r.output);
		break;
	case 4:
		fprintf(st_r.output, ".branch_compact");
		fflush(st_r.output);
		break;
	default: assert(0);
	}
}

void st_scene_phase_scheme_int(stscene *s)
{
	sffield *field;
	switch (s->state) {
	case 0:
		if (st_r.verbose) {
			fprintf(st_r.output, ".scheme_u32");
			fflush(st_r.output);
		}
		field = sf_fieldnew(&st_r.a, "key");
		t( field != NULL );
		t( sf_fieldoptions(field, &st_r.a, "u32,key(0)") == 0);
		t( sf_schemeadd(&st_r.scheme, &st_r.a, field) == 0);

		field = sf_fieldnew(&st_r.a, "value");
		t( field != NULL );
		t( sf_fieldoptions(field, &st_r.a, "string") == 0);
		t( sf_schemeadd(&st_r.scheme, &st_r.a, field) == 0);

		t( sp_setstring(st_r.env, "db.test.scheme", "key", 0) == 0 );
		t( sp_setstring(st_r.env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
		t( sp_setstring(st_r.env, "db.test.scheme", "value", 0) == 0 );
		t( sp_setstring(st_r.env, "db.test.scheme.value", "string", 0) == 0 );
		break;
	case 1:
		if (st_r.verbose) {
			fprintf(st_r.output, ".scheme_u64");
			fflush(st_r.output);
		}
		field = sf_fieldnew(&st_r.a, "key");
		t( field != NULL );
		t( sf_fieldoptions(field, &st_r.a, "u64,key(0)") == 0);
		t( sf_schemeadd(&st_r.scheme, &st_r.a, field) == 0);

		field = sf_fieldnew(&st_r.a, "value");
		t( field != NULL );
		t( sf_fieldoptions(field, &st_r.a, "string") == 0);
		t( sf_schemeadd(&st_r.scheme, &st_r.a, field) == 0);

		t( sp_setstring(st_r.env, "db.test.scheme", "key", 0) == 0 );
		t( sp_setstring(st_r.env, "db.test.scheme.key", "u64,key(0)", 0) == 0 );
		t( sp_setstring(st_r.env, "db.test.scheme", "value", 0) == 0 );
		t( sp_setstring(st_r.env, "db.test.scheme.value", "string", 0) == 0 );
		break;
	case 2:
		if (st_r.verbose) {
			fprintf(st_r.output, ".scheme_u32_u32");
			fflush(st_r.output);
		}
		field = sf_fieldnew(&st_r.a, "key");
		t( field != NULL );
		t( sf_fieldoptions(field, &st_r.a, "u32,key(0)") == 0);
		t( sf_schemeadd(&st_r.scheme, &st_r.a, field) == 0);

		field = sf_fieldnew(&st_r.a, "key_b");
		t( field != NULL );
		t( sf_fieldoptions(field, &st_r.a, "u32,key(0)") == 0);
		t( sf_schemeadd(&st_r.scheme, &st_r.a, field) == 0);

		field = sf_fieldnew(&st_r.a, "value");
		t( field != NULL );
		t( sf_fieldoptions(field, &st_r.a, "string") == 0);
		t( sf_schemeadd(&st_r.scheme, &st_r.a, field) == 0);

		t( sp_setstring(st_r.env, "db.test.scheme", "key", 0) == 0 );
		t( sp_setstring(st_r.env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
		t( sp_setstring(st_r.env, "db.test.scheme", "key_b", 0) == 0 );
		t( sp_setstring(st_r.env, "db.test.scheme.key_b", "u32,key(1)", 0) == 0 );
		t( sp_setstring(st_r.env, "db.test.scheme", "value", 0) == 0 );
		t( sp_setstring(st_r.env, "db.test.scheme.value", "string", 0) == 0 );
		break;
	default: assert(0);
	}
}

#if 0
void st_scene_phase_scheme(stscene *s)
{
	srkey *part;
	switch (s->state) {
	case 0:
		if (st_r.verbose) {
			fprintf(st_r.output, ".scheme_u32");
			fflush(st_r.output);
		}
		part = sr_schemeadd(&st_r.scheme);
		t( sr_keysetname(part, &st_r.a, "key") == 0 );
		t( sr_keyset(part, &st_r.a, "u32") == 0 );
		t( sp_setstring(st_r.env, "db.test.index.key", "u32", 0) == 0 );
		break;
	case 1:
		if (st_r.verbose) {
			fprintf(st_r.output, ".scheme_u64");
			fflush(st_r.output);
		}
		part = sr_schemeadd(&st_r.scheme);
		t( sr_keysetname(part, &st_r.a, "key") == 0 );
		t( sr_keyset(part, &st_r.a, "u64") == 0 );
		t( sp_setstring(st_r.env, "db.test.index.key", "u64", 0) == 0 );
		break;
	case 2:
		if (st_r.verbose) {
			fprintf(st_r.output, ".scheme_string");
			fflush(st_r.output);
		}
		part = sr_schemeadd(&st_r.scheme);
		t( sr_keysetname(part, &st_r.a, "key") == 0 );
		t( sr_keyset(part, &st_r.a, "string") == 0 );
		t( sp_setstring(st_r.env, "db.test.index.key", "string", 0) == 0 );
		break;
	case 3:
		if (st_r.verbose) {
			fprintf(st_r.output, ".scheme_u32_u32");
			fflush(st_r.output);
		}
		part = sr_schemeadd(&st_r.scheme);
		t( sr_keysetname(part, &st_r.a, "key") == 0 );
		t( sr_keyset(part, &st_r.a, "u32") == 0 );
		part = sr_schemeadd(&st_r.scheme);
		t( sr_keysetname(part, &st_r.a, "key_b") == 0 );
		t( sr_keyset(part, &st_r.a, "u32") == 0 );
		t( sp_setstring(st_r.env, "db.test.index.key", "u32", 0) == 0 );
		t( sp_setstring(st_r.env, "db.test.index", "key_b", 0) == 0 );
		t( sp_setstring(st_r.env, "db.test.index.key_b", "u32", 0) == 0 );
		break;
	case 4:
		if (st_r.verbose) {
			fprintf(st_r.output, ".scheme_string_u32");
			fflush(st_r.output);
		}
		part = sr_schemeadd(&st_r.scheme);
		t( sr_keysetname(part, &st_r.a, "key") == 0 );
		t( sr_keyset(part, &st_r.a, "string") == 0 );
		part = sr_schemeadd(&st_r.scheme);
		t( sr_keysetname(part, &st_r.a, "key_b") == 0 );
		t( sr_keyset(part, &st_r.a, "u32") == 0 );
		t( sp_setstring(st_r.env, "db.test.index.key", "string", 0) == 0 );
		t( sp_setstring(st_r.env, "db.test.index", "key_b", 0) == 0 );
		t( sp_setstring(st_r.env, "db.test.index.key_b", "u32", 0) == 0 );
		break;
	default: assert(0);
	}
}
#endif

void st_scene_phase_storage(stscene *s)
{
	switch (s->state) {
	case 0:
		if (st_r.verbose) {
			fprintf(st_r.output, ".storage_default");
			fflush(st_r.output);
		}
		break;
	case 1:
		if (st_r.verbose) {
			fprintf(st_r.output, ".storage_in_memory");
			fflush(st_r.output);
		}
		t( sp_setstring(st_r.env, "db.test.storage", "in-memory", 0) == 0 );
		break;
	case 2:
		if (st_r.verbose) {
			fprintf(st_r.output, ".storage_compression_cold");
			fflush(st_r.output);
		}
		t( sp_setstring(st_r.env, "db.test.compression_cold", "lz4", 0) == 0 );
		t( sp_setstring(st_r.env, "db.test.compression_hot", "none", 0) == 0 );
		break;
	case 3:
		if (st_r.verbose) {
			fprintf(st_r.output, ".storage_compression_hot");
			fflush(st_r.output);
		}
		t( sp_setstring(st_r.env, "db.test.compression_cold", "none", 0) == 0 );
		t( sp_setstring(st_r.env, "db.test.compression_hot", "lz4", 0) == 0 );
		break;
	case 4:
		if (st_r.verbose) {
			fprintf(st_r.output, ".storage_compression_full");
			fflush(st_r.output);
		}
		t( sp_setstring(st_r.env, "db.test.compression_cold", "lz4", 0) == 0 );
		t( sp_setstring(st_r.env, "db.test.compression_hot", "lz4", 0) == 0 );
		break;
	case 5:
		if (st_r.verbose) {
			fprintf(st_r.output, ".storage_mmap");
			fflush(st_r.output);
		}
		t( sp_setint(st_r.env, "db.test.mmap", 1) == 0 );
		break;
	case 6:
		if (st_r.verbose) {
			fprintf(st_r.output, ".storage_mmap_compression_cold");
			fflush(st_r.output);
		}
		t( sp_setint(st_r.env, "db.test.mmap", 1) == 0 );
		t( sp_setstring(st_r.env, "db.test.compression_cold", "lz4", 0) == 0 );
		break;
	case 7:
		if (st_r.verbose) {
			fprintf(st_r.output, ".storage_compression_copy");
			fflush(st_r.output);
		}
		t( sp_setint(st_r.env, "db.test.compression_copy", 1) == 0 );
		break;
	case 8:
		if (st_r.verbose) {
			fprintf(st_r.output, ".storage_mmap_compression_copy");
			fflush(st_r.output);
		}
		t( sp_setint(st_r.env, "db.test.mmap", 1) == 0 );
		t( sp_setint(st_r.env, "db.test.compression_copy", 1) == 0 );
		break;
	case 9:
		if (st_r.verbose) {
			fprintf(st_r.output, ".storage_compression_compression_copy");
			fflush(st_r.output);
		}
		t( sp_setstring(st_r.env, "db.test.compression_cold", "lz4", 0) == 0 );
		t( sp_setstring(st_r.env, "db.test.compression_hot", "lz4", 0) == 0 );
		t( sp_setint(st_r.env, "db.test.compression_copy", 1) == 0 );
		break;
	case 10:
		if (st_r.verbose) {
			fprintf(st_r.output, ".storage_mmap_compression_compression_copy");
			fflush(st_r.output);
		}
		t( sp_setint(st_r.env, "db.test.mmap", 1) == 0 );
		t( sp_setstring(st_r.env, "db.test.compression_cold", "lz4", 0) == 0 );
		t( sp_setint(st_r.env, "db.test.compression_copy", 1) == 0 );
		break;
	case 11:
		if (st_r.verbose) {
			fprintf(st_r.output, ".storage_in_memory_mmap");
			fflush(st_r.output);
		}
		t( sp_setstring(st_r.env, "db.test.storage", "in-memory", 0) == 0 );
		t( sp_setint(st_r.env, "db.test.mmap", 1) == 0 );
		break;
	case 12:
		if (st_r.verbose) {
			fprintf(st_r.output, ".storage_in_memory_compression");
			fflush(st_r.output);
		}
		t( sp_setstring(st_r.env, "db.test.storage", "in-memory", 0) == 0 );
		t( sp_setstring(st_r.env, "db.test.compression_cold", "lz4", 0) == 0 );
		break;
	case 13:
		if (st_r.verbose) {
			fprintf(st_r.output, ".storage_in_memory_compression_compression_copy");
			fflush(st_r.output);
		}
		t( sp_setstring(st_r.env, "db.test.storage", "in-memory", 0) == 0 );
		t( sp_setstring(st_r.env, "db.test.compression_cold", "lz4", 0) == 0 );
		t( sp_setint(st_r.env, "db.test.compression_copy", 1) == 0 );
		break;
	case 14:
		if (st_r.verbose) {
			fprintf(st_r.output, ".storage_amqf");
			fflush(st_r.output);
		}
		t( sp_setint(st_r.env, "db.test.amqf", 1) == 0 );
		break;
	default: assert(0);
	}
}

void st_scene_phase_size(stscene *s)
{
	switch (s->state) {
	case 0:
		if (st_r.verbose) {
			fprintf(st_r.output, ".size_8byte");
			fflush(st_r.output);
		}
		st_r.value_start = 8;
		st_r.value_end = 8;
		break;
	case 1:
		if (st_r.verbose) {
			fprintf(st_r.output, ".size_1Kb");
			fflush(st_r.output);
		}
		st_r.value_start = 1024;
		st_r.value_end = 1024;
		break;
	case 2:
		if (st_r.verbose) {
			fprintf(st_r.output, ".size_512Kb");
			fflush(st_r.output);
		}
		st_r.value_start = 512 * 1024;
		st_r.value_end = 512 * 1024;
		break;
	default: assert(0);
	}
}
