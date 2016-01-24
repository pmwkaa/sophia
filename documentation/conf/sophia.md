
Sophia Environment
------------------

| name | type | description  |
|---|---|---|
| sophia.version | string, ro | Get current Sophia version. |
| sophia.version\_storage | string, ro  | Get current Sophia storage version. |
| sophia.build | string, ro | Get git commit id of a current build. |
| sophia.error | string, ro | Get last error description. |
| sophia.path | string  | Set current Sophia environment directory. |
| sophia.path\_create | int | Fail if **sophia.path** directory is not exists. |
| sophia.recover | int | Recovery mode 1. on-phase (default) 2. - two-phase, 3. - three-phase. |
