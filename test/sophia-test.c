
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

/* std */
extern stgroup *ss_leb128_group(void);
extern stgroup *ss_pager_group(void);
extern stgroup *ss_a_group(void);
extern stgroup *ss_aslab_group(void);
extern stgroup *ss_order_group(void);
extern stgroup *ss_rq_group(void);
extern stgroup *ss_qf_group(void);
extern stgroup *ss_ht_group(void);
extern stgroup *ss_zstdfilter_group(void);
extern stgroup *ss_lz4filter_group(void);

/* runtime */
extern stgroup *sr_conf_group(void);
extern stgroup *sr_scheme_group(void);

/* version */
extern stgroup *sv_v_group(void);
extern stgroup *sv_index_group(void);
extern stgroup *sv_indexiter_group(void);
extern stgroup *sv_mergeiter_group(void);
extern stgroup *sv_writeiter_group(void);
extern stgroup *sl_group(void);
extern stgroup *sl_iter_group(void);
extern stgroup *sd_build_group(void);
extern stgroup *sd_v_group(void);
extern stgroup *sd_read_group(void);
extern stgroup *sd_pageiter_group(void);

/* generic */
extern stgroup *conf_group(void);
extern stgroup *cache_group(void);
extern stgroup *error_group(void);
extern stgroup *method_group(void);
extern stgroup *profiler_group(void);
extern stgroup *repository_group(void);
extern stgroup *shutdown_group(void);
extern stgroup *drop_group(void);
extern stgroup *ddl_group(void);
extern stgroup *multipart_group(void);
extern stgroup *tpr_group(void);
extern stgroup *document_group(void);
extern stgroup *env_group(void);
extern stgroup *deadlock_group(void);
extern stgroup *scheme_group(void);
extern stgroup *rev_group(void);
extern stgroup *backup_group(void);
extern stgroup *snapshot_group(void);
extern stgroup *anticache_group(void);
extern stgroup *checkpoint_group(void);
extern stgroup *view_db_group(void);
extern stgroup *view_group(void);
extern stgroup *view_cursor_group(void);
extern stgroup *prefix_group(void);
extern stgroup *transaction_md_group(void);
extern stgroup *transaction_misc_group(void);
extern stgroup *cursor_md_group(void);
extern stgroup *cursor_rc_group(void);
extern stgroup *half_commit_group(void);
extern stgroup *upsert_group(void);
extern stgroup *async_group(void);
extern stgroup *get_cache_group(void);
extern stgroup *github_group(void);

/* compaction */
extern stgroup *branch_group(void);
extern stgroup *compact_group(void);
extern stgroup *compact_delete_group(void);
extern stgroup *gc_group(void);
extern stgroup *lru_group(void);

/* functional */
extern stgroup *transaction_group(void);
extern stgroup *hermitage_group(void);
extern stgroup *cursor_group(void);

/* crash */
extern stgroup *durability_group(void);
extern stgroup *oom_group(void);
extern stgroup *io_group(void);
extern stgroup *recover_loop_group(void);

/* multithread */
extern stgroup *multithread_group(void);
extern stgroup *multithread_upsert_group(void);
extern stgroup *multithread_be_group(void);
extern stgroup *multithread_be_multipass_group(void);

/* memory */
extern stgroup *leak_group(void);

static void
usage(char *path, int error) {
	printf("sophia test-suite.\n");
	printf("\n");
	printf("usage: %s [-vhrlFPGTt] [options]\n", path);
	printf("  -F         run full tests (fast by default)\n");
	printf("  -t <id>    test id to execute\n");
	printf("  -l <file>  output file\n");
	printf("  -T         stop after test\n");
	printf("  -G         stop after group\n");
	printf("  -P         stop after plan\n");
	printf("  -v         verbose\n");
	printf("  -h         help\n");
	exit(error);
}

int
main(int argc, char *argv[])
{
	stconf conf = {
		.sophia_dir = "_test_sophia",
		.backup_dir = "_test_backup",
		.log_dir    = "_test_log",
		.db_dir     = "_test_db",
		.verbose    = 0,
		.report     = 0,
		.id         = NULL,
		.logfile    = NULL,
		.stop_plan  = 0,
		.stop_group = 0,
		.stop_test  = 0
	};
	int full = 0;
	int opt;
	while ((opt = getopt(argc, argv, "t:FPGTl:rvh")) != -1) {
		switch (opt) {
		case 'F': full = 1;
			break;
		case 'P': conf.stop_plan = 1;
			break;
		case 'G': conf.stop_group = 1;
			break;
		case 'T': conf.stop_test = 1;
			break;
		case 't': conf.id = optarg;
			break;
		case 'r': conf.report = 1;
			break;
		case 'l': conf.logfile = optarg;
			break;
		case 'v': conf.verbose = 1;
			break;
		case 'h': usage(argv[0], 0);
			break;
		default:  usage(argv[0], 1);
		}
	}
	st_init(&conf);

	st_suiteadd_scene(&st_r.suite, st_scene("rmrf", st_scene_rmrf, 1));
	st_suiteadd_scene(&st_r.suite, st_scene("test", st_scene_test, 1));
	st_suiteadd_scene(&st_r.suite, st_scene("pass", st_scene_pass, 1));
	st_suiteadd_scene(&st_r.suite, st_scene("init", st_scene_init, 1));
	st_suiteadd_scene(&st_r.suite, st_scene("scheme_u32", st_scene_scheme_u32, 1));
	st_suiteadd_scene(&st_r.suite, st_scene("rt", st_scene_rt, 1));
	st_suiteadd_scene(&st_r.suite, st_scene("gc", st_scene_gc, 1));
	st_suiteadd_scene(&st_r.suite, st_scene("env", st_scene_env, 1));
	st_suiteadd_scene(&st_r.suite, st_scene("branch_wm_1", st_scene_branch_wm_1, 1));
	st_suiteadd_scene(&st_r.suite, st_scene("thread_5", st_scene_thread_5, 1));
	st_suiteadd_scene(&st_r.suite, st_scene("phase_compaction", st_scene_phase_compaction, 5));
	st_suiteadd_scene(&st_r.suite, st_scene("phase_scheme", st_scene_phase_scheme, 5));
	st_suiteadd_scene(&st_r.suite, st_scene("phase_scheme_int", st_scene_phase_scheme_int, 3));
	st_suiteadd_scene(&st_r.suite, st_scene("phase_storage", st_scene_phase_storage, 14));
	st_suiteadd_scene(&st_r.suite, st_scene("phase_format", st_scene_phase_format, 2));
	st_suiteadd_scene(&st_r.suite, st_scene("phase_size", st_scene_phase_size, 3));
	st_suiteadd_scene(&st_r.suite, st_scene("open", st_scene_open, 1));
	st_suiteadd_scene(&st_r.suite, st_scene("destroy", st_scene_destroy, 1));
	st_suiteadd_scene(&st_r.suite, st_scene("truncate", st_scene_truncate, 1));
	st_suiteadd_scene(&st_r.suite, st_scene("recover", st_scene_recover, 1));

	stplan *plan;
	plan = st_plan("unit");
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rmrf"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "init"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "scheme_u32"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rt"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "test"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "gc"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "pass"));
	st_planadd(plan, ss_leb128_group());
	st_planadd(plan, ss_pager_group());
	st_planadd(plan, ss_a_group());
	st_planadd(plan, ss_aslab_group());
	st_planadd(plan, ss_order_group());
	st_planadd(plan, ss_rq_group());
	st_planadd(plan, ss_qf_group());
	st_planadd(plan, ss_ht_group());
	st_planadd(plan, ss_zstdfilter_group());
	st_planadd(plan, ss_lz4filter_group());
	st_planadd(plan, sr_conf_group());
	st_planadd(plan, sr_scheme_group());
	st_planadd(plan, sv_v_group());
	st_planadd(plan, sv_index_group());
	st_planadd(plan, sv_indexiter_group());
	st_planadd(plan, sv_mergeiter_group());
	st_planadd(plan, sv_writeiter_group());
	st_planadd(plan, sl_group());
	st_planadd(plan, sl_iter_group());
	st_planadd(plan, sd_build_group());
	st_planadd(plan, sd_v_group());
	st_planadd(plan, sd_read_group());
	st_planadd(plan, sd_pageiter_group());
	st_suiteadd(&st_r.suite, plan);

	plan = st_plan("generic");
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rmrf"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "init"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rt"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "test"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "gc"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "pass"));
	st_planadd(plan, conf_group());
	st_planadd(plan, cache_group());
	st_planadd(plan, error_group());
	st_planadd(plan, method_group());
	st_planadd(plan, profiler_group());
	st_planadd(plan, repository_group());
	st_planadd(plan, shutdown_group());
	st_planadd(plan, drop_group());
	st_planadd(plan, ddl_group());
	st_planadd(plan, multipart_group());
	st_planadd(plan, tpr_group());
	st_planadd(plan, env_group());
	st_planadd(plan, document_group());
	st_planadd(plan, deadlock_group());
	st_planadd(plan, scheme_group());
	st_planadd(plan, rev_group());
	st_planadd(plan, backup_group());
	st_planadd(plan, snapshot_group());
	st_planadd(plan, anticache_group());
	st_planadd(plan, checkpoint_group());
	st_planadd(plan, view_db_group());
	st_planadd(plan, view_group());
	st_planadd(plan, view_cursor_group());
	st_planadd(plan, prefix_group());
	st_planadd(plan, transaction_md_group());
	st_planadd(plan, transaction_misc_group());
	st_planadd(plan, half_commit_group());
	st_planadd(plan, cursor_md_group());
	st_planadd(plan, cursor_rc_group());
	st_planadd(plan, upsert_group());
	st_planadd(plan, async_group());
	st_planadd(plan, get_cache_group());
	st_planadd(plan, github_group());
	st_suiteadd(&st_r.suite, plan);

	plan = st_plan("compaction");
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rmrf"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "init"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rt"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "test"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "gc"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "pass"));
	st_planadd(plan, branch_group());
	st_planadd(plan, compact_group());
	st_planadd(plan, compact_delete_group());
	st_planadd(plan, gc_group());
	st_planadd(plan, lru_group());
	st_suiteadd(&st_r.suite, plan);

	if (! full) {
		plan = st_plan("functional_truncate_recover_fast");
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rmrf"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "init"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "scheme_u32"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rt"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "env"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "branch_wm_1"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "phase_format"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "phase_storage"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "phase_compaction"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "open"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "test"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "truncate"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "test"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "truncate"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "destroy"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "env"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "branch_wm_1"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "open"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "test"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "destroy"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "gc"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "pass"));
		st_planadd(plan, hermitage_group());
		st_planadd(plan, transaction_group());
		st_planadd(plan, cursor_group());
		st_suiteadd(&st_r.suite, plan);
	} else {
		plan = st_plan("functional_truncate_recover_full");
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rmrf"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "init"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "phase_size"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rt"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "env"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "branch_wm_1"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "phase_scheme_int"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "phase_format"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "phase_storage"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "phase_compaction"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "open"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "test"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "truncate"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "test"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "truncate"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "destroy"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "env"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "branch_wm_1"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "open"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "test"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "destroy"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "gc"));
		st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "pass"));
		st_planadd(plan, hermitage_group());
		st_planadd(plan, transaction_group());
		st_planadd(plan, cursor_group());
		st_suiteadd(&st_r.suite, plan);
	}

#if 0
	plan = st_plan("functional_full");
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rmrf"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "init"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "phase_size"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rt"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "env"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "branch_wm_1"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "phase_scheme_int"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "phase_format"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "phase_storage"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "phase_compaction"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "open"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "test"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "destroy"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "gc"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "pass"));
	st_planadd(plan, transaction_group());
	st_planadd(plan, cursor_group());
	st_suiteadd(&st_r.suite, plan);

	plan = st_plan("functional_truncate_full");
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rmrf"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "init"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "phase_size"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rt"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "env"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "branch_wm_1"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "phase_scheme_int"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "phase_format"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "phase_storage"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "phase_compaction"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "open"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "test"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "truncate"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "test"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "truncate"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "test"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "destroy"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "gc"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "pass"));
	st_planadd(plan, transaction_group());
	st_planadd(plan, cursor_group());
	st_suiteadd(&st_r.suite, plan);
#endif

	plan = st_plan("memory");
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rmrf"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "init"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rt"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "test"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "gc"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "pass"));
	st_planadd(plan, leak_group());
	st_suiteadd(&st_r.suite, plan);

	plan = st_plan("crash");
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rmrf"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "init"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rt"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "test"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "gc"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "pass"));
	st_planadd(plan, durability_group());
	st_planadd(plan, oom_group());
	st_planadd(plan, io_group());
	st_planadd(plan, recover_loop_group());
	st_suiteadd(&st_r.suite, plan);

	plan = st_plan("multithread");
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rmrf"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "init"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rt"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "test"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "gc"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "pass"));
	st_planadd(plan, multithread_be_group());
	st_planadd(plan, multithread_upsert_group());
	st_planadd(plan, multithread_group());
	st_suiteadd(&st_r.suite, plan);

	plan = st_plan("multithread_3x_run");
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rmrf"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "init"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "rt"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "env"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "thread_5"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "open"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "test"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "destroy"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "recover"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "env"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "thread_5"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "open"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "test"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "destroy"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "recover"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "env"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "thread_5"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "open"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "test"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "destroy"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "gc"));
	st_planadd_scene(plan, st_suitescene_of(&st_r.suite, "pass"));
	st_planadd(plan, multithread_be_multipass_group());
	st_suiteadd(&st_r.suite, plan);

	st_run();
	st_free();
	return 0;
}
