import gratis.Server;

/**
 * The entry point. It stays in the default package so that {@code java -cp classes Main} starts every version of
 * Gratis, which is how bench/Dockerfile runs old refs too. The server itself is in package gratis.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        Server.main(args);
    }
}
