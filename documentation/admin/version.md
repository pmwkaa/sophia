
Version
-------

Sophia has two versions: *release version* and *storage format version*.

Release version is the current version number. Sophia uses a common practice
for version naming: **major**.**minor**.**latest\_fix_number**.

Current release version can be read from **sophia.version** variable.

Storage format version follows the same rule. The number of version is equal to the
previous Sophia version, that contains modified storage format.

Current storage format version can be read from **sophia.version_storage** variable.

Any Sophia releases are storage format compatible if storage versions are equal.
