
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

int si_backup(si *index, sr *r, sdc *c, siplan *plan)
{
	sinode *node = plan->node;
	sd_creset(c);

	char dest[1024];
	snprintf(dest, sizeof(dest), "%s/%" PRIu32 ".incomplete/%s",
	         index->conf->path_backup,
	         (uint32_t)plan->a,
	         index->conf->name);

	/* read origin file */
	int rc = sr_bufensure(&c->c, r->a, node->file.size);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "%s", "memory allocation failed");
		sr_error_recoverable(r->e);
		return -1;
	}
	rc = sr_filepread(&node->file, 0, c->c.s, node->file.size);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' read error: %s",
		         node->file.file, strerror(errno));
		sr_error_recoverable(r->e);
		return -1;
	}
	sr_bufadvance(&c->c, node->file.size);

	/* copy */
	srpath path;
	sr_pathA(&path, dest, node->self.id.id, ".db");
	srfile file;
	sr_fileinit(&file, r->a);
	rc = sr_filenew(&file, path.path);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "backup db file '%s' create error: %s",
		         path.path, strerror(errno));
		sr_error_recoverable(r->e);
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
