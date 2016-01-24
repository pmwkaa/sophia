
Deadlocks
---------

Due to a nature of multi-statement transactions deadlocks are possible.
Deadlocks are not automatically handled. Transaction object procedure **deadlock**
can be used to check if the transaction is in deadlock.

When a deadlock happens, transactions stays in *Lock* state.

Example:

```C
void *db = sp_getobject(env, "db.database");

void *a = sp_begin(env);
void *b = sp_begin(env);

uint32_t key = 7;
void *o = sp_document(db);
sp_setstring(o, "key", &key, sizeof(key));
sp_set(a, o);
key = 8;
o = sp_document(db);
sp_setstring(o, "key", &key, sizeof(key));
sp_set(b, o);
o = sp_document(db);
sp_setstring(o, "key", &key, sizeof(key));
sp_set(a, o);
key = 7;
o = sp_document(db);
sp_setstring(o, "key", &key, sizeof(key));
sp_set(b, o);

sp_commit(a) == 2; /* lock */
sp_commit(b) == 2; /* lock */

sp_getint(a, "deadlock") == 1;
sp_getint(b, "deadlock") == 1;

sp_destroy(a);
sp_getint(b, "deadlock") == 0;

sp_commit(b) == 0; /* ok */
```
