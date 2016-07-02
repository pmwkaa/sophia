
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sophia.h>

static pthread_t  spr_thread;
static void      *spr_env;
static void      *spr_db;
static int        spr_start;
static int        spr_pause;
static int        spr_exit;
static int        spr_info;
static int        spr_info_interval;

static inline
void *spr_worker(void *arg) 
{
	char value[80];
	memset(value, 0, sizeof(value));
	int seq = 0;
	while (spr_start)
	{
		if (spr_pause) {
			sleep(1);
			continue;
		}
		char key[32];
		int key_size = snprintf(key, sizeof(key), "key:%d", seq);
		void *o = sp_document(spr_db);
		sp_setstring(o, "key", key, key_size);
		sp_setstring(o, "value", value, sizeof(value));
		int rc = sp_set(spr_db, o);
		if (rc == -1) {
			printf("sp_set() error\n");
			return NULL;
		}
		seq++;
	}
	return NULL;
}

static inline int
spr_cmd_start(void)
{
	/* create env */
	spr_env = sp_env();
	sp_setstring(spr_env, "sophia.path", "_test_sophia", 0);
	sp_setstring(spr_env, "backup.path", "_test_backup", 0);
	sp_setstring(spr_env, "db", "test", 0);

	sp_setstring(spr_env, "db.test.scheme", "key", 0);
	sp_setstring(spr_env, "db.test.scheme.key", "u32,key(0)", 0);
	sp_setstring(spr_env, "db.test.scheme", "value", 0);
	sp_setstring(spr_env, "db.test.scheme.value", "string", 0);
	sp_setstring(spr_env, "db.test.scheme", "ttl", 0);
	sp_setstring(spr_env, "db.test.scheme.ttl", "u32,timestamp,expire", 0);

	/*sp_setint(spr_env, "db.test.expire", 10);*/

	spr_db = sp_getobject(spr_env, "db.test");
	int rc;
	rc = sp_open(spr_env);
	if (rc == -1) {
		sp_destroy(spr_env);
		return -1;
	}
	/* start thread */
	spr_start = 1;
	pthread_create(&spr_thread, NULL, spr_worker, NULL);
	return 0;
}

static inline void
spr_cmd_stop(void)
{
	if (! spr_start)
		return;
	spr_start = 0;
	pthread_join(spr_thread, NULL);
	if (spr_env)
		sp_destroy(spr_env);
}

static inline void
spr_cmd_info(void)
{
	if (! spr_start)
		return;
	void *cur = sp_getobject(spr_env, NULL);
	void *o = NULL;
	while ((o = sp_get(cur, o)))
	{
		char *key = sp_getstring(o, "key", 0);
		char *value = sp_getstring(o, "value", 0);
		printf("%s = %s\n", key, (value) ? value : "");
	}
	sp_destroy(cur);
}

static inline void
spr_cmd_checkpoint(void)
{
	if (! spr_start)
		return;
	sp_setint(spr_env, "db.test.compaction.checkpoint", 0);
}

static inline void
spr_cmd_snapshot(void)
{
	if (! spr_start)
		return;
	sp_setint(spr_env, "db.test.compaction.snapshot", 0);
}

static inline void
spr_cmd_gc(void)
{
	if (! spr_start)
		return;
	sp_setint(spr_env, "db.test.compaction.gc", 0);
}

static inline void
spr_cmd_expire(void)
{
	if (! spr_start)
		return;
	sp_setint(spr_env, "db.test.compaction.expire", 0);
}

static inline void
spr_cmd_backup(void)
{
	if (! spr_start)
		return;
	sp_setint(spr_env, "backup.run", 0);
}

static inline void spr_cmd_help(void)
{
	printf(" sta[r]t    -- start profiling (create env)\n");
	printf(" stop       -- stop profiling\n");
	printf(" [p]ause    -- pause/continue profiling\n");
	printf(" [c]ontinue -- continue profiling\n");
	printf(" [i]nfo     -- show sophia statistics\n");
	printf(" checkpoint -- schedule checkpoint operation\n");
	printf(" snapshot   -- schedule snapshot operation\n");
	printf(" gc         -- schedule garbage collection\n");
	printf(" expire     -- schedule expire operation\n");
	printf(" backup     -- schedule backup operation\n");
	printf(" help       -- this help\n");
	printf(" exit       -- stop and quit\n");
}

static inline int
spr_argv(char *buf, int size, char *argv[8])
{
	int i = size;
	while (i >= 0) {
		if (buf[i] == '\n')
			break;
		i--;
	}
	buf[i] = 0;
	i = 0;
	char *p;
	for (p = strtok(buf, " \t");
	     p && i < 8;
	     p = strtok(NULL, " \t"))
	{
		argv[i] = p;
		i++;
	}
	return i;
}

static inline void
spr_execute(char *cmd, int size)
{
	char *argv[8];
	int argc;
	argc = spr_argv(cmd, size, argv);
	if (argc == 0)
		return;
	/* match command */
	if (strcmp(argv[0], "i") == 0 ||
	    strcmp(argv[0], "info") == 0) {
		spr_cmd_info();
	} else
	if (strcmp(argv[0], "checkpoint") == 0) {
		spr_cmd_checkpoint();
		printf("checkpoint is in progress\n");
	} else
	if (strcmp(argv[0], "snapshot") == 0) {
		spr_cmd_snapshot();
		printf("snapshot is in progress\n");
	} else
	if (strcmp(argv[0], "gc") == 0) {
		spr_cmd_gc();
		printf("gc is in progress\n");
	} else
	if (strcmp(argv[0], "expire") == 0) {
		spr_cmd_expire();
		printf("expire is in progress\n");
	} else
	if (strcmp(argv[0], "backup") == 0) {
		spr_cmd_backup();
		printf("backup is in progress\n");
	} else
	if (strcmp(argv[0], "r") == 0 ||
	    strcmp(argv[0], "start") == 0) {
		if (spr_start) {
			printf("profiling is already started\n");
			return;
		}
		int rc = spr_cmd_start();
		if (rc == -1)
			printf("start failed\n");
		printf("profiling started\n");
	} else
	if (strcmp(argv[0], "stop") == 0) {
		if (! spr_start) {
			printf("profiling is not started\n");
			return;
		}
		spr_cmd_stop();
		printf("profiling stopped\n");
	} else
	if (strcmp(argv[0], "p") == 0 ||
	    strcmp(argv[0], "pause") == 0) {
		if (spr_pause)
			printf("continue\n");
		else
			printf("on pause\n");
		spr_pause = !spr_pause;
	} else
	if (strcmp(argv[0], "c") == 0 ||
	    strcmp(argv[0], "continue") == 0) {
		if (spr_pause) {
			printf("continue\n");
			spr_pause = 0;
		}
	} else
	if (strcmp(argv[0], "exit") == 0) {
		printf("shutdown\n");
		spr_cmd_stop();
		spr_exit = 1;
	} else
	if (strcmp(argv[0], "help") == 0) {
		spr_cmd_help();
	} else {
		printf("unknown command '%s'\n", argv[0]);
		return;
	}
}

static inline void spr_tick(void)
{
	/*
	if (spr_info == spr_info_interval) {
		spr_cmd_info();
		spr_info = 0;
	}
	spr_info++;
	*/
}

static inline void spr_prompt(void)
{
	printf("> ");
	fflush(NULL);
}

static inline void spr_mainloop(void)
{
	fd_set rd;
	FD_SET(0 /* stdin */, &rd);
	spr_prompt();
	while (! spr_exit)
	{
		char buf[100];
		struct timeval tv;
		tv.tv_sec  = 1;
		tv.tv_usec = 0;
		fd_set frd = rd;
		int rc = select(1, &frd, NULL, NULL, &tv);
		switch (rc) {
		case -1: continue;
		case  0:
			spr_tick();
			continue;
		case  1:
			rc = read(0, buf, sizeof(buf));
			if (rc == -1)
				continue;
			if (rc == 0) {
				rc = 5;
				memcpy(buf, "exit", rc);
			}
			spr_execute(buf, rc);
			spr_prompt();
			break;
		}
	}
}

int
main(int argc, char *argv[])
{
	spr_start         = 0;
	spr_pause         = 0;
	spr_exit          = 0;
	spr_info          = 0;
	spr_info_interval = 5;
	spr_env           = NULL;
	spr_db            = NULL;
	printf("sophia profiler.\n");
	printf("Type 'help' for more information.\n");
	spr_mainloop();
	return 0;
}
