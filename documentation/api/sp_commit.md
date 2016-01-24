
**NAME**

sp\_commit - commit a multi-statement transaction

**SYNOPSIS**

```C
#include <sophia.h>

int sp_commit(void *transaction);
```

**DESCRIPTION**

sp\_commit(**transaction**): commit a transaction

The [sp\_commit()](sp_commit.md) function is used to apply changes of a multi-statement
transaction. All modifications that were made during the transaction
are written to the log file in a single batch.

If commit failed, transaction modifications are discarded.

For additional information take a look at [Transactions](../crud/transactions.md) and
[Deadlock](../crud/deadlocks.md) sections.

**EXAMPLE**

```C
void *a = sp_getobject(env, "db.database_a");
void *b = sp_getobject(env, "db.database_b");

char key[] = "hello";
char value[] = "world";

/* begin a transaction */
void *transaction = sp_begin(env);

void *o = sp_document(a);
sp_setstring(o, "key", key, sizeof(key));
sp_setstring(o, "value", value, sizeof(value));
sp_set(transaction, o);

o = sp_document(b);
sp_setstring(o, "key", key, sizeof(key));
sp_setstring(o, "value", value, sizeof(value));
sp_set(transaction, o);

/* complete */
sp_commit(transaction);
```

**RETURN VALUE**

On success, [sp\_commit()](sp_commit.md) returns 0. On error, it returns -1.
On rollback 1 is returned, 2 on lock.

**SEE ALSO**

[Sophia API](../tutorial/api.md)
