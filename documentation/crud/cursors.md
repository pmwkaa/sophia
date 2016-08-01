
Cursors
-------

It is possible to do range queries using cursors.

To create a cursor the [sp_cursor()](../api/sp_cursor.md), function should be used.
Each call to [sp_get()](../api/sp_get.md) on cursor object makes an iteration step according to
current iteration order. Second and futher [sp_get()](../api/sp_get.md) calls must
using previously get object to continue iteration.

To set iteration order, cursor key object must be prepared by setting **order** field.
To do prefix scan **prefix** field must be set using the same argument convention as for key.
Supported orders: **>**, **>=**, **<**, **<=** both including or excluding search key.
By default key order is set to >=.

If search key is not set, then maximum or minimum key is returned.

Example (traverse a database in increasing order):

```C
void *cursor = sp_cursor(env);
void *o = sp_document(db);
sp_setstring(o, "order", ">=", 0);
while ((o = sp_get(cursor, o))) {
	char *key = sp_getstring(o, "key", NULL);
	char *value = sp_getstring(o, "value", NULL);
	printf("%s = %s\n", key, value);
}
sp_destroy(cursor);
```

Cursors are consistent. It is possible to do iteration and deletions or updates
at the same time without any interference with query data or other transactions.

Cursor should be freed using the [sp_destroy()](../api/sp_destroy.md)
function after usage.
