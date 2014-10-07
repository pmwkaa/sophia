#ifndef ST_SCENE_H_
#define ST_SCENE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

void st_scene_rmrf(stscene*, stc*);
void st_scene_create(stscene*, stc*);
void st_scene_multithread(stscene*, stc*);
void st_scene_open(stscene*, stc*);
void st_scene_phase(stscene*, stc*);
void st_scene_truncate(stscene*, stc*);
void st_scene_destroy(stscene*, stc*);
void st_scene_pass(stscene*, stc*);
void st_scene_test(stscene*, stc*);
void st_scene_rerun(stscene*, stc*);

#endif
