IF (NOT WITH_VALGRIND)
    UNITTEST_FOR(ydb/core/tx/schemeshard)

    FORK_SUBTESTS()

    SPLIT_FACTOR(20)

    IF (SANITIZER_TYPE OR WITH_VALGRIND)
        TIMEOUT(3600)
        SIZE(LARGE)
        TAG(ya:fat)
    ELSE()
        TIMEOUT(600)
        SIZE(MEDIUM)
    ENDIF()

    PEERDIR(
        library/cpp/getopt
        library/cpp/regex/pcre
        library/cpp/svnversion
        ydb/core/testlib/pg
        ydb/core/tx
        ydb/core/tx/schemeshard/ut_helpers
        ydb/library/yql/public/udf/service/exception_policy
    )

    YQL_LAST_ABI_VERSION()

    SRCS(
        ut_split_merge.cpp
    )

    END()
ENDIF()
