package gratis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class TransactionsTest extends ServerTest {

    @Test
    void execWithoutMultiIsAnError() throws Exception {
        assertEquals("-ERR EXEC without MULTI\r\n", client().call("EXEC"));
    }

    @Test
    void emptyTransactionGivesAnEmptyArray() throws Exception {
        RespClient a = client();
        assertEquals(OK, a.call("MULTI"));
        assertEquals("*0\r\n", a.call("EXEC"));
        assertEquals("-ERR EXEC without MULTI\r\n", a.call("EXEC"));
    }

    @Test
    void queuedCommandsDoNotRunBeforeExec() throws Exception {
        RespClient a = client();
        RespClient b = client();
        assertEquals(OK, a.call("MULTI"));
        assertEquals(QUEUED, a.call("SET", "q", "41"));
        assertEquals(QUEUED, a.call("INCR", "q"));
        assertEquals(NIL, b.call("GET", "q"));
    }

    @Test
    void execRunsTheQueueInOrder() throws Exception {
        RespClient a = client();
        a.call("MULTI");
        a.call("SET", "e", "41");
        a.call("INCR", "e");
        assertEquals(array(OK, integer(42)), a.call("EXEC"));
        assertEquals(bulk("42"), a.call("GET", "e"));
    }

    @Test
    void discardDropsTheQueue() throws Exception {
        RespClient a = client();
        a.call("MULTI");
        a.call("SET", "d", "1");
        assertEquals(OK, a.call("DISCARD"));
        assertEquals(NIL, a.call("GET", "d"));
        assertEquals("-ERR DISCARD without MULTI\r\n", a.call("DISCARD"));
    }

    @Test
    void aFailingCommandBecomesAnErrorEntryAndTheRestStillRun() throws Exception {
        RespClient a = client();
        a.call("MULTI");
        a.call("SET", "f", "xyz");
        a.call("INCR", "f");
        a.call("SET", "g", "7");
        assertEquals(QUEUED, a.call("GET"));
        String reply = a.call("EXEC");
        String head = "*4\r\n" + OK + NOT_AN_INTEGER + OK + "-ERR ";
        assertTrue(reply.startsWith(head) && reply.endsWith("\r\n"), reply);
        assertEquals(bulk("7"), a.call("GET", "g"));
    }

    @Test
    void twoClientsHaveSeparateTransactions() throws Exception {
        RespClient a = client();
        RespClient b = client();
        a.call("MULTI");
        a.call("SET", "m", "1");
        b.call("MULTI");
        b.call("INCR", "m");
        assertEquals(array(OK), a.call("EXEC"));
        assertEquals(array(integer(2)), b.call("EXEC"));
    }

    @Test
    void nestedMultiIsAnErrorAndDoesNotEndTheTransaction() throws Exception {
        RespClient a = client();
        a.call("MULTI");
        assertEquals("-ERR MULTI calls can not be nested\r\n", a.call("MULTI"));
        assertEquals(QUEUED, a.call("PING"));
        assertEquals(array("+PONG\r\n"), a.call("EXEC"));
        assertEquals("+PONG\r\n", a.call("PING"));
    }

    @Test
    void blockingCommandsInsideExecReturnNilAtOnce() throws Exception {
        RespClient a = client();
        RespClient b = client();
        a.call("MULTI");
        a.call("BLPOP", "empty", "0");
        a.call("XREAD", "BLOCK", "0", "STREAMS", "nostream", "0-0");
        assertEquals(array(NIL_ARRAY, NIL_ARRAY), a.call("EXEC"));
        assertEquals(integer(1), b.call("RPUSH", "empty", "x"));
        assertTrue(a.silentFor(Duration.ofMillis(200)), "the client was left waiting and got a late reply");
        assertEquals(integer(1), b.call("LLEN", "empty"));
    }
}
