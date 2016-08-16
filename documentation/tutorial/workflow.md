
Common Workflow
---------------

Basic workflow is simple:

1. create sophia environment [sp_env()](../api/sp_env.md)
2. set options using [sp_setint()](../api/sp_setint.md), [sp_setstring()](../api/sp_setstring.md), define **sophia.path**
3. define databases
4. set expected [cache size](../admin/memory_requirements.md) **db.name.compaction.cache**
5. [sp_open()](../api/sp_open.md) environment
6. do transaction processing using [sp_document()](../api/sp_document.md), [sp_set()](../api/sp_set.md), [sp_get()](../api/sp_get.md),
   [sp_delete()](../api/sp_delete.md), [sp_upsert()](../api/sp_upsert.md), [sp_cursor()](../api/sp_cursor.md), [sp_begin()](../api/sp_begin.md),
   [sp_commit()](../api/sp_commit.md), [sp_destroy()](../api/sp_destroy.md)
7. on finish: [sp_destroy()](../api/sp_destroy.md) the environment object

```C
void *env = sp_env();
sp_setstring(env, "sophia.path", "./storage", 0);
sp_setstring(env, "db", "test", 0);
sp_setint(env, "db.test.compaction.cache", 4ULL * 1024 * 1024 * 1024);
sp_open(env);
void *db = sp_getobject(env, "db.test");
/* do transactions */
sp_destroy(env);
```
