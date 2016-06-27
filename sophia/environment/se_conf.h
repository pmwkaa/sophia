#ifndef SE_CONF_H_
#define SE_CONF_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct seconfrt seconfrt;
typedef struct seconf seconf;

struct seconfrt {
	/* sophia */
	char     version[16];
	char     version_storage[16];
	char     build[32];
	/* error */
	uint64_t errors;
	/* scheduler */
	uint32_t backup_active;
	uint32_t backup_last;
	uint32_t backup_last_complete;
	/* log */
	uint32_t log_files;
	/* metric */
	srseq    seq;
	/* transaction */
	srstatxm tx_stat;
	uint32_t tx_ro;
	uint32_t tx_rw;
	uint32_t tx_gc;
	uint64_t tx_vlsn;
};

struct seconf {
	uint32_t  threads;
	sfscheme  scheme;
	int       confmax;
	srconf   *conf;
	so       *env;
};

int      se_confinit(seconf*, so*);
void     se_conffree(seconf*);
int      se_confvalidate(seconf*);
int      se_confserialize(seconf*, ssbuf*);
int      se_confset_string(so*, const char*, void*, int);
int      se_confset_int(so*, const char*, int64_t);
void    *se_confget_object(so*, const char*);
void    *se_confget_string(so*, const char*, int*);
int64_t  se_confget_int(so*, const char*);

#endif
