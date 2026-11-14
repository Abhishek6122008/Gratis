# Gratis at any git ref, built from that ref's sources. build-image.sh passes a git worktree as the build context,
# so this file (from the current checkout) also builds refs that predate it, like v1-baseline.
# Base images pinned by digest: Temurin 25.0.5+8, the same builds bench/profile.sh uses.
FROM eclipse-temurin:25-jdk@sha256:83f4a7fc27df94dcc7b8de04d14cbbba8da14406f921e966969c3b09000aa507 AS build
COPY src/main/java /src
RUN mkdir /classes && javac -d /classes $(find /src -name '*.java')

FROM eclipse-temurin:25-jre@sha256:ecdc0adb37bde1a281c4d245a3cd535004b1fc8322d560f5b09463b248c81df0
COPY --from=build /classes /app
# The same heap for every ref, so a run compares code, not JVM defaults. v1 ignores --port and uses 6379 anyway.
# GRATIS_ARGS passes server options (run.sh sets --shards from a target like gratis@REF,shards=4); refs that don't
# know an option ignore it.
ENV JAVA_OPTS="-Xms1g -Xmx1g"
ENV GRATIS_ARGS=""
EXPOSE 6379
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -cp /app Main --port 6379 $GRATIS_ARGS"]
