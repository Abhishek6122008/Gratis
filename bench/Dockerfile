# Gratis at any git ref, built from that ref's sources. build-image.sh passes a git worktree as the build context,
# so this file (from the current checkout) also builds refs that predate it, like v1-baseline.
FROM eclipse-temurin:25-jdk AS build
COPY src/main/java /src
RUN mkdir /classes && javac -d /classes $(find /src -name '*.java')

FROM eclipse-temurin:25-jre
COPY --from=build /classes /app
# The same heap for every ref, so a run compares code, not JVM defaults. v1 ignores --port and uses 6379 anyway.
ENV JAVA_OPTS="-Xms1g -Xmx1g"
EXPOSE 6379
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -cp /app Main --port 6379"]
