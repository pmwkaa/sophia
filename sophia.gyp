{
  'variables': {
    'target_arch%': 'ia32',              # set target architecture
    'host_arch%': 'ia32',                # set host architecture
    'sophia_library%': 'static_library', # allow override to 'shared_library' for DLL/.so builds
  },

  'target_defaults': {
    'default_configuration': 'Release',
    'configurations': {
      'Debug': {
        'defines': [ 'DEBUG' ],
        'cflags': [
	    '-std=c99',
	    '-pedantic',
	    '-Wall',
	    '-Wextra',
	    '-fvisibility=hidden',
	    '-g',
	    '-O0'
	],
        'xcode_settings': {
          'GCC_OPTIMIZATION_LEVEL': '0',
        },
      },
      'Release': {
        'defines': [ 'NDEBUG' ],
        'cflags': [
	    '-std=c99',
	    '-pedantic',
	    '-Wall',
	    '-Wextra',
	    '-fvisibility=hidden',
	    '-O2'
	],
      }
    },
    'conditions': [
      ['OS in "freebsd linux openbsd solaris android"', {
        'target_conditions': [
          ['_type=="static_library"', {
            'standalone_static_library': 1, # disable thin archive which needs binutils >= 2.19
          }],
        ],
        'conditions': [
          [ 'host_arch != target_arch and target_arch=="ia32"', {
            'cflags': [ '-m32' ],
            'ldflags': [ '-m32' ],
          }],
          [ 'OS=="solaris"', {
            'cflags': [ '-pthreads' ],
            'ldflags': [ '-pthreads' ],
          }],
          [ 'OS != "solaris"', {
            'cflags': [ '-pthread' ],
            'ldflags': [ '-pthread' ],
          }],
        ],
      }],
      ['OS=="mac"', {
        'xcode_settings': {
          'ALWAYS_SEARCH_USER_PATHS': 'NO',
          'GCC_CW_ASM_SYNTAX': 'NO',                # No -fasm-blocks
          'GCC_DYNAMIC_NO_PIC': 'NO',               # No -mdynamic-no-pic (Equivalent to -fPIC)
          'GCC_ENABLE_CPP_EXCEPTIONS': 'NO',        # -fno-exceptions
          'GCC_ENABLE_CPP_RTTI': 'NO',              # -fno-rtti
          'GCC_ENABLE_PASCAL_STRINGS': 'NO',        # No -mpascal-strings
          'GCC_INLINES_ARE_PRIVATE_EXTERN': 'YES',  # -fvisibility-inlines-hidden
          'GCC_SYMBOLS_PRIVATE_EXTERN': 'YES',      # -fvisibility=hidden
          'GCC_THREADSAFE_STATICS': 'NO',           # -fno-threadsafe-statics
          'GCC_WARN_ABOUT_MISSING_NEWLINE': 'YES',  # -Wnewline-eof
          'PREBINDING': 'NO',                       # No -Wl,-prebind
          'USE_HEADERMAP': 'NO',
        },
        'conditions': [
          ['target_arch=="ia32"', {
            'xcode_settings': {'ARCHS': ['i386']},
          }],
          ['target_arch=="x64"', {
            'xcode_settings': {'ARCHS': ['x86_64']},
          }],
        ],
        'target_conditions': [
          ['_type!="static_library"', {
            'xcode_settings': {'OTHER_LDFLAGS': ['-Wl,-search_paths_first']},
          }],
        ],
      }],
    ],
  },

  'targets': [
    {
      'target_name': 'sophia',
      'product_prefix': 'lib',
      'type': '<(sophia_library)',
      'include_dirs': ['db'],
      'link_settings': {
        'libraries': ['-lpthread'],
      },
      'direct_dependent_settings': {
        'include_dirs': ['db'],
      },
      'conditions': [
        ['sophia_library=="shared_library"', {
            'cflags': [ '-fPIC' ],
        }],
      ],
      'sources': [
        'db/cat.c',
        'db/crc.c',
        'db/cursor.c',
        'db/e.c',
        'db/file.c',
        'db/gc.c',
        'db/i.c',
        'db/merge.c',
        'db/recover.c',
        'db/rep.c',
        'db/sp.c',
        'db/util.c',
      ],
    },
  ],
}
