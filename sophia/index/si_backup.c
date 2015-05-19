
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsd.h>
#include <libsi.h>

static inline int
si_backupend(si *index, sr *r, sdc *c, siplan *plan)
{
	/* copy index scheme file */
	char src[PATH_MAX];
	snprintf(src, sizeof(src), "%s/scheme", index->scheme->path);

	char dst[PATH_MAX];
	snprintf(dst, sizeof(dst), "%s/%" PRIu32 ".incomplete/%s/scheme",
	         index->scheme->path_backup,
	         (uint32_t)plan->a,
	         index->scheme->name);

	/* prepare buffer */
	ssize_t size = sr_filesize(src);
	if (srunlikely(size == -1)) {
		sr_error(r->e, "backup db file '%s' read error: %s",
		         src, strerror(errno));
		return -1;
	}
	int rc = sr_bufensure(&c->c, r->a, size);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "%s", "memory allocation failed");
		return -1;
	}

	/* read scheme file */
	srfile file;
	sr_fileinit(&file, r->a);
	rc = sr_fileopen(&file, src);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "backup db file '%s' open error: %s",
		         src, strerror(errno));
		return -1;
	}
	rc = sr_filepread(&file, 0, c->c.s, size);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "backup db file '%s' read error: %s",
		         src, strerror(errno));
		sr_fileclose(&file);
		return -1;
	}
	sr_fileclose(&file);

	/* write scheme file */
	rc = sr_filenew(&file, dst);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "backup db file '%s' create error: %s",
		         dst, strerror(errno));
		return -1;
	}
	rc = sr_filewrite(&file, c->c.s, size);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "backup db file '%s' write error: %s",
		         dst, strerror(errno));
		sr_fileclose(&file);
		return -1;
	}
	/* sync? */
	rc = sr_fileclose(&file);
	if (srunlikely(rc == -1)) {
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

int si_backup(si *index, sr *r, sdc *c, siplan *plan)
{
	sd_creset(c);
	if (srunlikely(plan->plan == SI_BACKUPEND))
		return si_backupend(index, r, c, plan);

	sinode *node = plan->node;
	char dst[PATH_MAX];
	snprintf(dst, sizeof(dst), "%s/%" PRIu32 ".incomplete/%s",
	         index->scheme->path_backup,
	         (uint32_t)plan->a,
	         index->scheme->name);

	/* read origin file */
	int rc = sr_bufensure(&c->c, r->a, node->file.size);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "%s", "memory allocation failed");
		return -1;
	}
	rc = sr_filepread(&node->file, 0, c->c.s, node->file.size);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' read error: %s",
		         node->file.file, strerror(errno));
		return -1;
	}
	sr_bufadvance(&c->c, node->file.size);

	/* copy */
	srpath path;
	sr_pathA(&path, dst, node->self.id.id, ".db");
	srfile file;
	sr_fileinit(&file, r->a);
	rc = sr_filenew(&file, path.path);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "backup db file '%s' create error: %s",
		         path.path, strerror(errno));
		return -1;
	}
	rc = sr_filewrite(&file, c->c.s, node->file.size);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "backup db file '%s' write error: %s",
				 path.path, strerror(errno));
		sr_fileclose(&file);
		return -1;
	}
	/* sync? */
	rc = sr_fileclose(&file);
	if (srunlikely(rc == -1)) {
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
