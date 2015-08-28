
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libsv.h>
#include <libsd.h>
#include <libsi.h>

static inline int
si_backupend(si *index, sdc *c, siplan *plan)
{
	sr *r = index->r;
	/* copy index scheme file */
	char src[PATH_MAX];
	snprintf(src, sizeof(src), "%s/scheme", index->scheme->path);

	char dst[PATH_MAX];
	snprintf(dst, sizeof(dst), "%s/%" PRIu32 ".incomplete/%s/scheme",
	         index->scheme->path_backup,
	         (uint32_t)plan->a,
	         index->scheme->name);

	/* prepare buffer */
	ssize_t size = ss_filesize(src);
	if (ssunlikely(size == -1)) {
		sr_error(r->e, "backup db file '%s' read error: %s",
		         src, strerror(errno));
		return -1;
	}
	int rc = ss_bufensure(&c->c, r->a, size);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);

	/* read scheme file */
	ssfile file;
	ss_fileinit(&file, r->a);
	rc = ss_fileopen(&file, src);
	if (ssunlikely(rc == -1)) {
		sr_error(r->e, "backup db file '%s' open error: %s",
		         src, strerror(errno));
		return -1;
	}
	rc = ss_filepread(&file, 0, c->c.s, size);
	if (ssunlikely(rc == -1)) {
		sr_error(r->e, "backup db file '%s' read error: %s",
		         src, strerror(errno));
		ss_fileclose(&file);
		return -1;
	}
	ss_fileclose(&file);

	/* write scheme file */
	rc = ss_filenew(&file, dst);
	if (ssunlikely(rc == -1)) {
		sr_error(r->e, "backup db file '%s' create error: %s",
		         dst, strerror(errno));
		return -1;
	}
	rc = ss_filewrite(&file, c->c.s, size);
	if (ssunlikely(rc == -1)) {
		sr_error(r->e, "backup db file '%s' write error: %s",
		         dst, strerror(errno));
		ss_fileclose(&file);
		return -1;
	}
	/* sync? */
	rc = ss_fileclose(&file);
	if (ssunlikely(rc == -1)) {
		sr_error(r->e, "backup db file '%s' close error: %s",
		         dst, strerror(errno));
		return -1;
	}

	/* finish index backup */
	si_lock(index);
	index->backup = plan->a;
	si_unlock(index);
	return 0;
}

int si_backup(si *index, sdc *c, siplan *plan)
{
	sr *r = index->r;
	sd_creset(c);
	if (ssunlikely(plan->plan == SI_BACKUPEND))
		return si_backupend(index, c, plan);

	sinode *node = plan->node;
	char dst[PATH_MAX];
	snprintf(dst, sizeof(dst), "%s/%" PRIu32 ".incomplete/%s",
	         index->scheme->path_backup,
	         (uint32_t)plan->a,
	         index->scheme->name);

	/* read origin file */
	int rc = si_noderead(node, r, &c->c);
	if (ssunlikely(rc == -1))
		return -1;

	/* copy */
	sspath path;
	ss_pathA(&path, dst, node->self.id.id, ".db");
	ssfile file;
	ss_fileinit(&file, r->a);
	rc = ss_filenew(&file, path.path);
	if (ssunlikely(rc == -1)) {
		sr_error(r->e, "backup db file '%s' create error: %s",
		         path.path, strerror(errno));
		return -1;
	}
	rc = ss_filewrite(&file, c->c.s, node->file.size);
	if (ssunlikely(rc == -1)) {
		sr_error(r->e, "backup db file '%s' write error: %s",
				 path.path, strerror(errno));
		ss_fileclose(&file);
		return -1;
	}
	/* sync? */
	rc = ss_fileclose(&file);
	if (ssunlikely(rc == -1)) {
		sr_error(r->e, "backup db file '%s' close error: %s",
				 path.path, strerror(errno));
		return -1;
	}

	si_lock(index);
	node->backup = plan->a;
	si_nodeunlock(node);
	si_unlock(index);
	return 0;
}
