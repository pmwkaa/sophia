
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

int exists(char *path, char *name) {
	char file[1024];
	snprintf(file, sizeof(file), "%s/%s", path, name);
	return ss_fileexists(file);
}

int rmrf(char *path)
{
	DIR *d = opendir(path);
	if (d == NULL)
		return -1;
	char file[1024];
	struct dirent *de;
	while ((de = readdir(d))) {
		if (de->d_name[0] == '.')
			continue;
		snprintf(file, sizeof(file), "%s/%s", path, de->d_name);
		int rc;
		if (de->d_type == DT_DIR) {
			rc = rmrf(file);
		} else {
			rc = unlink(file);
		}
		if (rc == -1) {
			closedir(d);
			return -1;
		}
	}
	closedir(d);
	return rmdir(path);
}
