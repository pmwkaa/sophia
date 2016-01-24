
**NAME**

sp\_open - open or create

**SYNOPSIS**

```C
#include <sophia.h>

int sp_open(void *object);
```

**DESCRIPTION**

sp\_open(**env**): create environment, open or create pre-defined databases.

sp\_open(**database**): create or open database.

Please take a look at [Configuration](../tutorial/configuration.md),
and [Database](../admin/database.md) administration sections.

Common workflow is described [here](../tutorial/workflow.md).

**EXAMPLE**

```C
void *env = sp_env();
sp_setstring(env, "sophia.path", "./storage", 0);
sp_setstring(env, "db", "test", 0);
sp_open(env);
void *db = sp_getobject(env, "db.test");
/* do transactions */
sp_destroy(env);
```

**RETURN VALUE**

On success, [sp\_open()](sp_open.md) returns 0. On error, it returns -1.

**SEE ALSO**

[Sophia API](../tutorial/api.md)
