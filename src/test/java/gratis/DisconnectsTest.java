package gratis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.OutputStream;
import java.net.Socket;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/** A client that goes away, however it goes, only ends its own connection. */
class DisconnectsTest extends ServerTest {

    /** Sends a command, then closes with a TCP reset instead of a normal close. */
    private void sendAndReset(String... command) throws Exception {
        try (Socket socket = new Socket("127.0.0.1", port())) {
            socket.setSoLinger(true, 0);
            OutputStream out = socket.getOutputStream();
            out.write(RespClient.encode(command));
            out.flush();
            Thread.sleep(100);
        }
    }

    @Test
    void aClientThatResetsItsConnectionDoesNotStopTheServer() throws Exception {
        RespClient a = client();
        a.call("SET", "k", "v");
        sendAndReset("PING");
        Thread.sleep(200);
        assertEquals(bulk("v"), a.call("GET", "k"));
        assertEquals("+PONG\r\n", client().call("PING"));
    }

    @Test
    void aWaiterThatResetItsConnectionDoesNotTakeThePushedItem() throws Exception {
        sendAndReset("BLPOP", "l", "0");
        RespClient pusher = client();
        assertEquals(integer(1), pusher.call("RPUSH", "l", "x"));
        assertEquals(integer(1), pusher.call("LLEN", "l"));
    }

    @Test
    void aWaiterThatClosedNormallyDoesNotTakeThePushedItem() throws Exception {
        RespClient gone = client();
        gone.send(RespClient.encode("BLPOP", "l", "0"));
        gone.silentFor(Duration.ofMillis(100));
        gone.close();
        Thread.sleep(100);
        RespClient pusher = client();
        assertEquals(integer(1), pusher.call("RPUSH", "l", "x"));
        assertEquals(integer(1), pusher.call("LLEN", "l"));
    }
}
