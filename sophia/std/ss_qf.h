#ifndef SS_QF_H_
#define SS_QF_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssqf ssqf;

struct ssqf {
	uint8_t   qf_qbits;
	uint8_t   qf_rbits;
	uint8_t   qf_elem_bits;
	uint32_t  qf_entries;
	uint64_t  qf_index_mask;
	uint64_t  qf_rmask;
	uint64_t  qf_elem_mask;
	uint64_t  qf_max_size;
	uint32_t  qf_table_size;
	uint64_t *qf_table;
	ssbuf     qf_buf;
};

int  ss_qfinit(ssqf*);
int  ss_qfensure(ssqf*, ssa*, uint32_t);
void ss_qffree(ssqf*, ssa*);
void ss_qfreset(ssqf*);
void ss_qfadd(ssqf*, uint64_t);
int  ss_qfhas(ssqf*, uint64_t);

#endif
