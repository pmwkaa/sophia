
**NAME**

sp\_drop - schedule an database drop or an object deletion

**SYNOPSIS**

```C
#include <sophia.h>

int sp_drop(void *object);
```

**DESCRIPTION**

sp\_drop(**database**)
Schedule database drop.

sp\_drop(**view**)
Drop a view.

**EXAMPLE**

```C
void *db = sp_getobject(env, "db.test");
sp_drop(db);
```

**RETURN VALUE**

On success, [sp\_drop()](sp_drop.md) returns 0. On error, it returns -1.

**SEE ALSO**

[Sophia API](../tutorial/api.md)
