
Point-in-Time Views
-------------------

To create a View, new view name should be assigned to **view** configuration namespace.

It is possible to do [sp_get()](../api/sp_get.md) or [sp_cursor()](../api/sp_cursor.md) on a view object.

```C
sp_setstring(env, "view", "today", 0);
void *view = sp_getobject(env, "view.today");
```

Views are not persistent, therefore view object must be recreated after shutdown before opening
environment with latest view LSN number: **view.name.lsn**.

```C
sp_setstring(env, "view", "today", 0);
sp_setint(env, "view.today.lsn", 12345);
```

To delete a view, [sp_drop()](../api/sp_drop.md) or [sp_destroy()](../api/sp_destroy.md) should be called on a view object.
