
Database creation
-----------------

Database can be created, opened or deleted before or after environment startup.
To create a database, new database name should be set to *db* configuration namespace. 
If no database exists, it will be created automatically.

Sophia v2.2 does not save database scheme information.

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

Sophia stores data in rows. Each row has strictly typed fields.

Following field types are supported: **string**, **u64**, **u64\_rev**, **u32**, **u32\_rev**,
**u16**, **u16\_rev**, **u8**, **u8\_rev**.

Atleast one field must be specified as a **key(n)**. Where *n* is a key order position.
Several fields can be selected as an index primary key.

Example:

```C
void *env = sp_env();
sp_setstring(env, "db", "test", 0);
sp_setstring(env, "db.test.scheme", "id", 0);
sp_setstring(env, "db.test.scheme.id", "u32,key(0)", 0);
sp_setstring(env, "db.test.scheme", "data", 0);
sp_setstring(env, "db.test.scheme.data", "string", 0);
```

If scheme is not defined, following scheme will be automatically used:

```C
void *env = sp_env();
sp_setstring(env, "db", "test", 0);
sp_setstring(env, "db.test.scheme", "key", 0);
sp_setstring(env, "db.test.scheme.key", "string,key(0)", 0);
sp_setstring(env, "db.test.scheme", "value", 0);
sp_setstring(env, "db.test.scheme.value", "string, 0);
```

