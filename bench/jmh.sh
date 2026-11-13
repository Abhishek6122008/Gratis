#!/usr/bin/env bash
# Runs the JMH benchmarks in src/test/java/gratis/jmh.
# Usage: bench/jmh.sh [JMH options]     e.g. bench/jmh.sh ParseBenchmark -prof gc
# Needs JAVA_HOME (JDK 25), like ./mvnw.
set -euo pipefail
cd "$(dirname "$0")/.."
./mvnw -B -q test-compile dependency:build-classpath -Dmdep.outputFile=target/jmh.classpath -Dmdep.includeScope=test
exec "${JAVA_HOME:+$JAVA_HOME/bin/}java" -cp "target/test-classes:target/classes:$(cat target/jmh.classpath)" \
  org.openjdk.jmh.Main "$@"
