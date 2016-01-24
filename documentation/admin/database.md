
Database creation
-----------------

Database can be created, opened or deleted before or after environment startup.
To create a database, new database name should be set to *db* configuration namespace. 
If no database exists, it will be created automatically.

Sophia v2.1 does not save database scheme information.

By default databases are created in **sophia.path/database_name** folder.
It is possible to set custom folder per database using **db.database_name.path**
It might be useful to separate databases on different disk drives.

Create or open database before environment start:

```C
void *env = sp_env();
sp_setstring(env, "sophia.path", "./storage", 0);
sp_setstring(env, "db", "test", 0);
sp_open(env);
void *db = sp_getobject(env, "db.test");
sp_destroy(env);
```

Database schema
---------------

By default database index type is **string**.
Following index key types are supported: **string**, **u32**, **u64**, **u32\_rev**, **u64\_rev**.

```C
void *env = sp_env();
sp_setstring(env, "db.test.index.key", "u32", 0);
```

Sophia supports multi-part keys:

```C
void *env = sp_env();
sp_setstring(env, "db.test.index.key", "u32", 0);
sp_setstring(env, "db.test.index", "key_b", 0);
sp_setstring(env, "db.test.index.key_b", "string", 0);
...
void *o = sp_object(db);
sp_setstring(o, "key", &key_a, 0);
sp_setstring(o, "key_b", "hello", 0);
sp_set(db, o);
```

Online database creation
------------------------

Create or open database after environment start:

```C
void *env = sp_env();
sp_setstring(env, "sophia.path", "./storage", 0);
sp_open(env);
sp_setstring(env, "db", "test", 0);
void *db = sp_getobject(env, "db.test");
sp_open(db);
sp_destroy(db); /* close database */
sp_destroy(env);
```

Online database close and drop
------------------------------

To close a database the [sp_destroy()](../api/sp_destroy.md) method should be called on a database object.
Note that [sp_destroy()](../api/sp_destroy.md) does not delete any data.

To schedule a database drop the [sp_drop()](../api/sp_drop.md) method should be called on a database object.
Actuall drop procedure will be automatically scheduled when a latest
transaction completes.
