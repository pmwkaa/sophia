
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libst.h>
#include <sophia.h>

extern stgroup *srpager_group(void);
extern stgroup *sraslab_group(void);
extern stgroup *svindex_group(void);
extern stgroup *svindexiter_group(void);
extern stgroup *svmergeiter_group(void);
extern stgroup *svseaveiter_group(void);
extern stgroup *sl_group(void);
extern stgroup *sliter_group(void);
extern stgroup *sdbuild_group(void);
extern stgroup *sdv_group(void);
extern stgroup *sditer_group(void);
extern stgroup *sdpageiter_group(void);
extern stgroup *ctl_group(void);
extern stgroup *error_group(void);
extern stgroup *method_group(void);
extern stgroup *dml_group(void);
extern stgroup *tpr_group(void);
extern stgroup *object_group(void);
extern stgroup *profiler_group(void);
extern stgroup *transaction_group(void);
extern stgroup *deadlock_group(void);
extern stgroup *cursor_group(void);
extern stgroup *recoverloop_group(void);
extern stgroup *recovercrash_group(void);
extern stgroup *mt_group(void);
extern stgroup *mt_backend_group(void);

int
main(int argc, char *argv[])
{
	st s;
	st_init(&s, "./dir", "./logdir");

	st_addscene(&s, st_scene("rmrf", st_scene_rmrf, 1));
	st_addscene(&s, st_scene("create", st_scene_create, 1));
	st_addscene(&s, st_scene("multithread", st_scene_multithread, 1));
	st_addscene(&s, st_scene("open", st_scene_open, 1));
	st_addscene(&s, st_scene("phase", st_scene_phase, 5));
	st_addscene(&s, st_scene("truncate", st_scene_truncate, 1));
	st_addscene(&s, st_scene("destroy", st_scene_destroy, 1));
	st_addscene(&s, st_scene("pass", st_scene_pass, 1));
	st_addscene(&s, st_scene("rerun", st_scene_rerun, 1));
	st_addscene(&s, st_scene("test", st_scene_test, 1));

	stplan *plan;
	plan = st_plan("unit");
	st_planscene(plan, st_sceneof(&s, "rmrf"));
	st_planscene(plan, st_sceneof(&s, "test"));
	st_planscene(plan, st_sceneof(&s, "pass"));
	st_planadd(plan, srpager_group());
	st_planadd(plan, sraslab_group());
	st_planadd(plan, svindex_group());
	st_planadd(plan, svindexiter_group());
	st_planadd(plan, svmergeiter_group());
	st_planadd(plan, svseaveiter_group());
	st_planadd(plan, sl_group());
	st_planadd(plan, sliter_group());
	st_planadd(plan, sdbuild_group());
	st_planadd(plan, sdv_group());
	st_planadd(plan, sditer_group());
	st_planadd(plan, sdpageiter_group());
	st_add(&s, plan);

	plan = st_plan("separate");
	st_planscene(plan, st_sceneof(&s, "rmrf"));
	st_planscene(plan, st_sceneof(&s, "test"));
	st_planscene(plan, st_sceneof(&s, "pass"));
	st_planadd(plan, ctl_group());
	st_planadd(plan, error_group());
	st_planadd(plan, method_group());
	st_planadd(plan, profiler_group());
	st_planadd(plan, dml_group());
	st_planadd(plan, tpr_group());
	st_planadd(plan, deadlock_group());
	st_add(&s, plan);

	plan = st_plan("default");
	st_planscene(plan, st_sceneof(&s, "rmrf"));
	st_planscene(plan, st_sceneof(&s, "create"));
	st_planscene(plan, st_sceneof(&s, "phase"));
	st_planscene(plan, st_sceneof(&s, "open"));
	st_planscene(plan, st_sceneof(&s, "test"));
	st_planscene(plan, st_sceneof(&s, "destroy"));
	st_planscene(plan, st_sceneof(&s, "pass"));
	st_planadd(plan, object_group());
	st_planadd(plan, transaction_group());
	st_planadd(plan, cursor_group());
	st_add(&s, plan);

	plan = st_plan("truncate-repeat");
	st_planscene(plan, st_sceneof(&s, "rmrf"));
	st_planscene(plan, st_sceneof(&s, "create"));
	st_planscene(plan, st_sceneof(&s, "phase"));
	st_planscene(plan, st_sceneof(&s, "open"));
	st_planscene(plan, st_sceneof(&s, "test"));
	st_planscene(plan, st_sceneof(&s, "truncate"));
	st_planscene(plan, st_sceneof(&s, "test"));
	st_planscene(plan, st_sceneof(&s, "truncate"));
	st_planscene(plan, st_sceneof(&s, "test"));
	st_planscene(plan, st_sceneof(&s, "destroy"));
	st_planscene(plan, st_sceneof(&s, "pass"));
	st_planadd(plan, object_group());
	st_planadd(plan, transaction_group());
	st_planadd(plan, cursor_group());
	st_add(&s, plan);

	plan = st_plan("truncate-recover-repeat");
	st_planscene(plan, st_sceneof(&s, "rmrf"));
	st_planscene(plan, st_sceneof(&s, "create"));
	st_planscene(plan, st_sceneof(&s, "phase"));
	st_planscene(plan, st_sceneof(&s, "open"));
	st_planscene(plan, st_sceneof(&s, "test"));
	st_planscene(plan, st_sceneof(&s, "truncate"));
	st_planscene(plan, st_sceneof(&s, "destroy"));
	st_planscene(plan, st_sceneof(&s, "create"));
	st_planscene(plan, st_sceneof(&s, "open"));
	st_planscene(plan, st_sceneof(&s, "test"));
	st_planscene(plan, st_sceneof(&s, "destroy"));
	st_planscene(plan, st_sceneof(&s, "pass"));
	st_planadd(plan, object_group());
	st_planadd(plan, transaction_group());
	st_planadd(plan, cursor_group());
	st_add(&s, plan);

	plan = st_plan("recover_crash");
	st_planscene(plan, st_sceneof(&s, "rmrf"));
	st_planscene(plan, st_sceneof(&s, "test"));
	st_planscene(plan, st_sceneof(&s, "pass"));
	st_planadd(plan, recovercrash_group());
	st_add(&s, plan);

	plan = st_plan("recover_loop");
	st_planscene(plan, st_sceneof(&s, "rmrf"));
	st_planscene(plan, st_sceneof(&s, "test"));
	st_planscene(plan, st_sceneof(&s, "pass"));
	st_planadd(plan, recoverloop_group());
	st_add(&s, plan);

	plan = st_plan("multithreaded_frontend");
	st_planscene(plan, st_sceneof(&s, "rmrf"));
	st_planscene(plan, st_sceneof(&s, "test"));
	st_planscene(plan, st_sceneof(&s, "pass"));
	st_planadd(plan, mt_group());
	st_add(&s, plan);

	plan = st_plan("multithreaded_backend");
	st_planscene(plan, st_sceneof(&s, "rmrf"));
	st_planscene(plan, st_sceneof(&s, "create"));
	st_planscene(plan, st_sceneof(&s, "multithread"));
	st_planscene(plan, st_sceneof(&s, "open"));
	st_planscene(plan, st_sceneof(&s, "test"));
	st_planscene(plan, st_sceneof(&s, "destroy"));
	st_planscene(plan, st_sceneof(&s, "rerun"));
	st_planscene(plan, st_sceneof(&s, "create"));
	st_planscene(plan, st_sceneof(&s, "multithread"));
	st_planscene(plan, st_sceneof(&s, "open"));
	st_planscene(plan, st_sceneof(&s, "test"));
	st_planscene(plan, st_sceneof(&s, "destroy"));
	st_planscene(plan, st_sceneof(&s, "rerun"));
	st_planscene(plan, st_sceneof(&s, "create"));
	st_planscene(plan, st_sceneof(&s, "multithread"));
	st_planscene(plan, st_sceneof(&s, "open"));
	st_planscene(plan, st_sceneof(&s, "test"));
	st_planscene(plan, st_sceneof(&s, "destroy"));
	st_planscene(plan, st_sceneof(&s, "pass"));
	st_planadd(plan, mt_backend_group());
	st_add(&s, plan);

	st_run(&s);
	st_free(&s);
	return 0;
}
