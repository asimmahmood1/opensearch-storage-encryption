#/1bin/bash
#
time ./gradlew jmh -Pjmh.includes='PrefetchBufferpoolVsMMapBenchmark' -Pjmh.wi=1 -Pjmh.i=1 2>&1 -Pjmh.prof="async:libPath=${ASYNC_LIB};event=cpu;output=flamegraph;dir=profile-results" | grep -E '(Parameters|Iteration|STATS|Result|Benchmark |ops/ms|Fork|Warmup)'
