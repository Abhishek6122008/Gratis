# redis-java

Building my own Redis from scratch in Java 😈

## Run

```sh
./mvnw -B package
java -cp target/classes Main            # port 6379
java -cp target/classes Main --port 6380
```

## Test

```sh
./mvnw -B verify
```

The tests start a real server on a free port for each test and talk to it over a socket. Needs JDK 25.
