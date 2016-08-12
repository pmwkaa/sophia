#ifndef ST_SCENES_H_
#define ST_SCENES_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

void st_scene_rmrf(stscene*);
void st_scene_test(stscene*);
void st_scene_pass(stscene*);
void st_scene_init(stscene*);
void st_scene_scheme_u32(stscene*);
void st_scene_rt(stscene*);
void st_scene_gc(stscene*);

void st_scene_env(stscene*);
void st_scene_cache_0(stscene*);
void st_scene_thread_5(stscene*);
void st_scene_open(stscene*);
void st_scene_destroy(stscene*);
void st_scene_truncate(stscene*);
void st_scene_recover(stscene*);

void st_scene_phase_compaction(stscene*);
void st_scene_phase_scheme(stscene*);
void st_scene_phase_storage(stscene*);
void st_scene_phase_size(stscene*);

#endif
