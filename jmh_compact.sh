#/1bin/bash
#
#
export ASYNC_LIB="/home/asimmahm/workplace/async-profiler-4.3-linux-x64/lib/libasyncProfiler.so"

time ./gradlew jmh -Pjmh.includes='PrefetchBufferpoolVsMMapBenchmark' -Pjmh.wi=1 -Pjmh.i=2 -Pjmh.jvmArgs="-XX:MaxInlineLevel=15 -XX:FreqInlineSize=500 -XX:InlineSmallCode=4000" -Pjmh.prof="async:libPath=${ASYNC_LIB};event=cpu;output=flamegraph;dir=profile-results" 2>&1  | grep -E '(Parameters|Iteration|STATS|Result|Benchmark |ops/ms|Fork|Warmup)'
