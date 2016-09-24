
Memory Usage
------------

It is important to understand Sophia memory requirements.

Here are precalculated memory usage (cache size) for expected storage capacity and write rates.
They should be considered to correctly set `db.compaction.cache` variable.

Sequential Write: 100 MB/Sec (common HDD)
----------------------------

| Write Rate | 25 Gb | 60 Gb | 120 Gb | 200 Gb | 400 Gb | 700 Gb | 1000 Gb | 2000 Gb |
|---|---|---|---|---|---|---|---|---|
| 0.1 MB/s | 50 MB | 120 MB | 240 MB | 400 MB | 800 MB | 1401 MB | 2001 MB | 4003 MB |
| 0.5 MB/s | 257 MB | 617 MB | 1234 MB | 2058 MB | 4116 MB | 7204 MB | 10291 MB | 20582 MB |
| 1 MB/s | 517 MB | 1241 MB | 2482 MB | 4137 MB | 8274 MB | 14480 MB | 20686 MB | 41373 MB |
| 2 MB/s | 1044 MB | 2506 MB | 5014 MB | 8358 MB | 16718 MB | 29256 MB | 41794 MB | 83590 MB |
| 4 MB/s | 2132 MB | 5120 MB | 10240 MB | 17064 MB | 34132 MB | 59732 MB | 85332 MB | 170664 MB |
| 8 MB/s | 4448 MB | 10680 MB | 21368 MB | 35616 MB | 71232 MB | 124656 MB | 178080 MB | 356168 MB |
| 12 MB/s | 6972 MB | 16752 MB | 33504 MB | 55848 MB | 111708 MB | 195480 MB | 279264 MB | 558540 MB |
| 16 MB/s | 9744 MB | 23392 MB | 46800 MB | 78016 MB | 156032 MB | 273056 MB | 390080 MB | 780176 MB |
| 28 MB/s | 19908 MB | 47768 MB | 95564 MB | 159264 MB | 318556 MB | 557508 MB | 796432 MB | 1592864 MB |
| 40 MB/s | 34120 MB | 81920 MB | 163840 MB | 273040 MB | 546120 MB | 955720 MB | 1365320 MB | 2730640 MB |

Sequential Write: 200 MB/Sec (Flash)
----------------------------

| Write Rate | 25 Gb | 60 Gb | 120 Gb | 200 Gb | 400 Gb | 700 Gb | 1000 Gb | 2000 Gb |
|---|---|---|---|---|---|---|---|---|
| 0.1 MB/s | 25 MB | 59 MB | 120 MB | 200 MB | 400 MB | 700 MB | 1000 MB | 2000 MB |
| 0.5 MB/s | 128 MB | 307 MB | 615 MB | 1026 MB | 2053 MB | 3592 MB | 5132 MB | 10265 MB |
| 1 MB/s | 257 MB | 617 MB | 1234 MB | 2058 MB | 4116 MB | 7204 MB | 10291 MB | 20582 MB |
| 2 MB/s | 516 MB | 1240 MB | 2482 MB | 4136 MB | 8274 MB | 14480 MB | 20686 MB | 41372 MB |
| 4 MB/s | 1044 MB | 2504 MB | 5012 MB | 8356 MB | 16716 MB | 29256 MB | 41792 MB | 83588 MB |
| 8 MB/s | 2128 MB | 5120 MB | 10240 MB | 17064 MB | 34128 MB | 59728 MB | 85328 MB | 170664 MB |
| 12 MB/s | 3264 MB | 7836 MB | 15684 MB | 26136 MB | 52284 MB | 91500 MB | 130716 MB | 261444 MB |
| 16 MB/s | 4448 MB | 10672 MB | 21360 MB | 35616 MB | 71232 MB | 124656 MB | 178080 MB | 356160 MB |
| 28 MB/s | 8316 MB | 19992 MB | 39984 MB | 66668 MB | 133336 MB | 233352 MB | 333368 MB | 666764 MB |
| 40 MB/s | 12800 MB | 30720 MB | 61440 MB | 102400 MB | 204800 MB | 358400 MB | 512000 MB | 1024000 MB |

Sequential Write: 300 MB/Sec (Flash)
----------------------------

| Write Rate | 25 Gb | 60 Gb | 120 Gb | 200 Gb | 400 Gb | 700 Gb | 1000 Gb | 2000 Gb |
|---|---|---|---|---|---|---|---|---|
| 0.1 MB/s | 16 MB | 39 MB | 79 MB | 133 MB | 266 MB | 466 MB | 666 MB | 1333 MB |
| 0.5 MB/s | 85 MB | 205 MB | 410 MB | 683 MB | 1367 MB | 2393 MB | 3419 MB | 6838 MB |
| 1 MB/s | 171 MB | 410 MB | 821 MB | 1369 MB | 2739 MB | 4794 MB | 6849 MB | 13698 MB |
| 2 MB/s | 342 MB | 824 MB | 1648 MB | 2748 MB | 5496 MB | 9620 MB | 13744 MB | 27488 MB |
| 4 MB/s | 688 MB | 1660 MB | 3320 MB | 5532 MB | 11068 MB | 19372 MB | 27672 MB | 55348 MB |
| 8 MB/s | 1400 MB | 3360 MB | 6728 MB | 11216 MB | 22440 MB | 39272 MB | 56104 MB | 112216 MB |
| 12 MB/s | 2124 MB | 5112 MB | 10236 MB | 17064 MB | 34128 MB | 59724 MB | 85332 MB | 170664 MB |
| 16 MB/s | 2880 MB | 6912 MB | 13840 MB | 23072 MB | 46144 MB | 80752 MB | 115376 MB | 230752 MB |
| 28 MB/s | 5264 MB | 12628 MB | 25284 MB | 42140 MB | 84308 MB | 147560 MB | 210812 MB | 421624 MB |
| 40 MB/s | 7840 MB | 18880 MB | 37800 MB | 63000 MB | 126000 MB | 220520 MB | 315040 MB | 630120 MB |

Sequential Write: 400 MB/Sec (Flash)
----------------------------

| Write Rate | 25 Gb | 60 Gb | 120 Gb | 200 Gb | 400 Gb | 700 Gb | 1000 Gb | 2000 Gb |
|---|---|---|---|---|---|---|---|---|
| 0.1 MB/s | 12 MB | 29 MB | 59 MB | 100 MB | 200 MB | 350 MB | 500 MB | 1000 MB |
| 0.5 MB/s | 64 MB | 153 MB | 307 MB | 512 MB | 1025 MB | 1794 MB | 2563 MB | 5126 MB |
| 1 MB/s | 128 MB | 307 MB | 615 MB | 1026 MB | 2053 MB | 3592 MB | 5132 MB | 10265 MB |
| 2 MB/s | 256 MB | 616 MB | 1234 MB | 2058 MB | 4116 MB | 7204 MB | 10290 MB | 20582 MB |
| 4 MB/s | 516 MB | 1240 MB | 2480 MB | 4136 MB | 8272 MB | 14480 MB | 20684 MB | 41372 MB |
| 8 MB/s | 1040 MB | 2504 MB | 5008 MB | 8352 MB | 16712 MB | 29256 MB | 41792 MB | 83584 MB |
| 12 MB/s | 1572 MB | 3792 MB | 7596 MB | 12660 MB | 25332 MB | 44328 MB | 63336 MB | 126672 MB |
| 16 MB/s | 2128 MB | 5120 MB | 10240 MB | 17056 MB | 34128 MB | 59728 MB | 85328 MB | 170656 MB |
| 28 MB/s | 3836 MB | 9240 MB | 18480 MB | 30828 MB | 61656 MB | 107884 MB | 154140 MB | 308280 MB |
| 40 MB/s | 5680 MB | 13640 MB | 27280 MB | 45480 MB | 91000 MB | 159280 MB | 227520 MB | 455080 MB |
