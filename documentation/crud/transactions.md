
Transactions
------------

Sophia supports fast optimistic single-statement and multi-statement
transactions. Transactions are completely isolated from each other under
Serializable Snapshot isolation (SSI).

Single-statement transactions
-----------------------------

Single-statement transactions are automatically processed when
[sp_set()](../api/sp_set.md), [sp_delete()](../api/sp_delete.md),
[sp_upsert()](../api/sp_upsert.md), [sp_get()](../api/sp_get.md)
are used on a database object.

As a part of a transactional statement a key-value document must be prepared using
[sp_document()](../api/sp_document.md) method.

First argument of [sp_document()](../api/sp_document.md) method must be a database object.

Object must be prepared by setting **key** and **value** fields, where value is optional.
It is important that while setting **key** and **value** fields, only pointers are copied. Real
data copies only during first operation.

Prepared document is automatically freed on commit.

```C
void *db = sp_getobject(env, "db.test");
void *o = sp_document(db);
sp_setstring(o, "key", "hello", 0);
sp_setstring(o, "value", "world", 0));
sp_set(db, o); /* transaction */
o = sp_document(db);
sp_set(o, "key", "hello", 0);
sp_delete(db, o);
```

[sp_get(database)](../api/sp_get.md) method returns a document that is semantically equal to
[sp_document(database)](../api/sp_document.md), but is read-only.

Example:

```C
void *o = sp_document(db);
sp_set(o, "key", "hello", 0);
void *result = sp_get(db, o);
if (result) {
	int valuesize;
	char *value = sp_getstring(result, "value", &valuesize);
	printf("%s\n", value);
	sp_destroy(result);
}
```

Multi-statement transactions
----------------------------

Multi-statement transaction is automatically processed when
[sp_set()](../api/sp_set.md), [sp_delete()](../api/sp_delete.md), [sp_upsert()](../api/sp_upsert.md),
[sp_get()](../api/sp_get.md) are used on a transactional object.

The [sp_begin()](../api/sp_begin.md) function is used to start a multi-statement transaction.

During transaction, no updates are written to the database files until a [sp_commit()](../api/sp_commit.md) is called.
On commit, all modifications that were made are written to the log file in a single batch.

To discard any changes made during transaction operation, [sp_destroy()](../api/sp_destroy.md) function should be used.
No nested transactions are supported.

There are no limit on a number of concurrent transactions.
Any number of databases can be involved in a multi-statement transaction.

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

A transactional status should be checked (both for single and multi-statement):

```C
int status = sp_commit(transaction);
switch (status) {
case -1: /* error */
case  0: /* ok */
case  1: /* rollback */
case  2: /* lock */
}
```

*Rollback* status means that transaction has been rollbacked by another concurrent
transaction. *Lock* status means that transaction is not finished and waiting for concurrent
transaction to complete. In that case commit should be retried later or transaction can be
rollbacked. Any error happened during multi-statement transaction does not rollback a transaction.
