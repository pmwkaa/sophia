
Memory Usage
------------

It is important to understand Sophia memory requirements.

Here are precalculated memory usage (cache size) for expected storage capacity and write rates.
They should be considered to correctly set `db.compaction.cache` variable.

Sequential Write: 100 Mb/Sec (common HDD)
----------------------------

| Write Rate | 25 Gb | 60 Gb | 120 Gb | 200 Gb | 400 Gb | 700 Gb | 1000 Gb | 2000 Gb |
|---|---|---|---|---|---|---|---|---|
| 0.1 Mb/s | 50 Mb | 120 Mb | 240 Mb | 400 Mb | 800 Mb | 1401 Mb | 2001 Mb | 4003 Mb |
| 0.5 Mb/s | 257 Mb | 617 Mb | 1234 Mb | 2058 Mb | 4116 Mb | 7204 Mb | 10291 Mb | 20582 Mb |
| 1 Mb/s | 517 Mb | 1241 Mb | 2482 Mb | 4137 Mb | 8274 Mb | 14480 Mb | 20686 Mb | 41373 Mb |
| 2 Mb/s | 1044 Mb | 2506 Mb | 5014 Mb | 8358 Mb | 16718 Mb | 29256 Mb | 41794 Mb | 83590 Mb |
| 4 Mb/s | 2132 Mb | 5120 Mb | 10240 Mb | 17064 Mb | 34132 Mb | 59732 Mb | 85332 Mb | 170664 Mb |
| 8 Mb/s | 4448 Mb | 10680 Mb | 21368 Mb | 35616 Mb | 71232 Mb | 124656 Mb | 178080 Mb | 356168 Mb |
| 12 Mb/s | 6972 Mb | 16752 Mb | 33504 Mb | 55848 Mb | 111708 Mb | 195480 Mb | 279264 Mb | 558540 Mb |
| 16 Mb/s | 9744 Mb | 23392 Mb | 46800 Mb | 78016 Mb | 156032 Mb | 273056 Mb | 390080 Mb | 780176 Mb |
| 28 Mb/s | 19908 Mb | 47768 Mb | 95564 Mb | 159264 Mb | 318556 Mb | 557508 Mb | 796432 Mb | 1592864 Mb |
| 40 Mb/s | 34120 Mb | 81920 Mb | 163840 Mb | 273040 Mb | 546120 Mb | 955720 Mb | 1365320 Mb | 2730640 Mb |

Sequential Write: 200 Mb/Sec (Flash)
----------------------------

| Write Rate | 25 Gb | 60 Gb | 120 Gb | 200 Gb | 400 Gb | 700 Gb | 1000 Gb | 2000 Gb |
|---|---|---|---|---|---|---|---|---|
| 0.1 Mb/s | 25 Mb | 59 Mb | 120 Mb | 200 Mb | 400 Mb | 700 Mb | 1000 Mb | 2000 Mb |
| 0.5 Mb/s | 128 Mb | 307 Mb | 615 Mb | 1026 Mb | 2053 Mb | 3592 Mb | 5132 Mb | 10265 Mb |
| 1 Mb/s | 257 Mb | 617 Mb | 1234 Mb | 2058 Mb | 4116 Mb | 7204 Mb | 10291 Mb | 20582 Mb |
| 2 Mb/s | 516 Mb | 1240 Mb | 2482 Mb | 4136 Mb | 8274 Mb | 14480 Mb | 20686 Mb | 41372 Mb |
| 4 Mb/s | 1044 Mb | 2504 Mb | 5012 Mb | 8356 Mb | 16716 Mb | 29256 Mb | 41792 Mb | 83588 Mb |
| 8 Mb/s | 2128 Mb | 5120 Mb | 10240 Mb | 17064 Mb | 34128 Mb | 59728 Mb | 85328 Mb | 170664 Mb |
| 12 Mb/s | 3264 Mb | 7836 Mb | 15684 Mb | 26136 Mb | 52284 Mb | 91500 Mb | 130716 Mb | 261444 Mb |
| 16 Mb/s | 4448 Mb | 10672 Mb | 21360 Mb | 35616 Mb | 71232 Mb | 124656 Mb | 178080 Mb | 356160 Mb |
| 28 Mb/s | 8316 Mb | 19992 Mb | 39984 Mb | 66668 Mb | 133336 Mb | 233352 Mb | 333368 Mb | 666764 Mb |
| 40 Mb/s | 12800 Mb | 30720 Mb | 61440 Mb | 102400 Mb | 204800 Mb | 358400 Mb | 512000 Mb | 1024000 Mb |

Sequential Write: 300 Mb/Sec (Flash)
----------------------------

| Write Rate | 25 Gb | 60 Gb | 120 Gb | 200 Gb | 400 Gb | 700 Gb | 1000 Gb | 2000 Gb |
|---|---|---|---|---|---|---|---|---|
| 0.1 Mb/s | 16 Mb | 39 Mb | 79 Mb | 133 Mb | 266 Mb | 466 Mb | 666 Mb | 1333 Mb |
| 0.5 Mb/s | 85 Mb | 205 Mb | 410 Mb | 683 Mb | 1367 Mb | 2393 Mb | 3419 Mb | 6838 Mb |
| 1 Mb/s | 171 Mb | 410 Mb | 821 Mb | 1369 Mb | 2739 Mb | 4794 Mb | 6849 Mb | 13698 Mb |
| 2 Mb/s | 342 Mb | 824 Mb | 1648 Mb | 2748 Mb | 5496 Mb | 9620 Mb | 13744 Mb | 27488 Mb |
| 4 Mb/s | 688 Mb | 1660 Mb | 3320 Mb | 5532 Mb | 11068 Mb | 19372 Mb | 27672 Mb | 55348 Mb |
| 8 Mb/s | 1400 Mb | 3360 Mb | 6728 Mb | 11216 Mb | 22440 Mb | 39272 Mb | 56104 Mb | 112216 Mb |
| 12 Mb/s | 2124 Mb | 5112 Mb | 10236 Mb | 17064 Mb | 34128 Mb | 59724 Mb | 85332 Mb | 170664 Mb |
| 16 Mb/s | 2880 Mb | 6912 Mb | 13840 Mb | 23072 Mb | 46144 Mb | 80752 Mb | 115376 Mb | 230752 Mb |
| 28 Mb/s | 5264 Mb | 12628 Mb | 25284 Mb | 42140 Mb | 84308 Mb | 147560 Mb | 210812 Mb | 421624 Mb |
| 40 Mb/s | 7840 Mb | 18880 Mb | 37800 Mb | 63000 Mb | 126000 Mb | 220520 Mb | 315040 Mb | 630120 Mb |

Sequential Write: 400 Mb/Sec (Flash)
----------------------------

| Write Rate | 25 Gb | 60 Gb | 120 Gb | 200 Gb | 400 Gb | 700 Gb | 1000 Gb | 2000 Gb |
|---|---|---|---|---|---|---|---|---|
| 0.1 Mb/s | 12 Mb | 29 Mb | 59 Mb | 100 Mb | 200 Mb | 350 Mb | 500 Mb | 1000 Mb |
| 0.5 Mb/s | 64 Mb | 153 Mb | 307 Mb | 512 Mb | 1025 Mb | 1794 Mb | 2563 Mb | 5126 Mb |
| 1 Mb/s | 128 Mb | 307 Mb | 615 Mb | 1026 Mb | 2053 Mb | 3592 Mb | 5132 Mb | 10265 Mb |
| 2 Mb/s | 256 Mb | 616 Mb | 1234 Mb | 2058 Mb | 4116 Mb | 7204 Mb | 10290 Mb | 20582 Mb |
| 4 Mb/s | 516 Mb | 1240 Mb | 2480 Mb | 4136 Mb | 8272 Mb | 14480 Mb | 20684 Mb | 41372 Mb |
| 8 Mb/s | 1040 Mb | 2504 Mb | 5008 Mb | 8352 Mb | 16712 Mb | 29256 Mb | 41792 Mb | 83584 Mb |
| 12 Mb/s | 1572 Mb | 3792 Mb | 7596 Mb | 12660 Mb | 25332 Mb | 44328 Mb | 63336 Mb | 126672 Mb |
| 16 Mb/s | 2128 Mb | 5120 Mb | 10240 Mb | 17056 Mb | 34128 Mb | 59728 Mb | 85328 Mb | 170656 Mb |
| 28 Mb/s | 3836 Mb | 9240 Mb | 18480 Mb | 30828 Mb | 61656 Mb | 107884 Mb | 154140 Mb | 308280 Mb |
| 40 Mb/s | 5680 Mb | 13640 Mb | 27280 Mb | 45480 Mb | 91000 Mb | 159280 Mb | 227520 Mb | 455080 Mb |
