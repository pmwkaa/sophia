
**NAME**

sp\_env - create a new environment handle

**SYNOPSIS**

```C
#include <sophia.h>

void *sp_env(void);
```

**DESCRIPTION**

The sp\_env() function allocates new Sophia environment object.

The object is intended for usage by [sp\_open()](sp_open.md) and must be
configured first. After using, an object should be freed by [sp\_destroy()](sp_destroy.md).

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

On success, [sp\_env()](sp_env.md) allocates new environment object pointer.
On error, it returns NULL.

**SEE ALSO**

[Sophia API](../tutorial/api.md)
