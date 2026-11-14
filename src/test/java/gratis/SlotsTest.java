package gratis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** Key slots must match Redis Cluster's, so hash tags mean what Redis users expect. Expected values are Redis's. */
class SlotsTest {
    private static int slot(String key) {
        return Slots.slot(key.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void crc16MatchesTheClusterSpecTestVector() {
        byte[] digits = "123456789".getBytes(StandardCharsets.US_ASCII);
        assertEquals(0x31C3, Slots.crc16(digits, 0, digits.length));
    }

    @Test
    void slotsMatchRedisClusterKeyslot() {
        // The examples in Redis's CLUSTER KEYSLOT documentation.
        assertEquals(11058, slot("somekey"));
        assertEquals(2515, slot("foo{hash_tag}"));
        assertEquals(2515, slot("bar{hash_tag}"));
        assertEquals(12182, slot("foo"));
    }

    @Test
    void hashTagsFollowTheClusterSpec() {
        assertEquals(slot("{user1000}.following"), slot("{user1000}.followers"));
        assertEquals(slot("bar"), slot("foo{bar}{zap}"));     // only the first {...} counts
        assertEquals(slot("{bar"), slot("foo{{bar}}zap"));    // from the first '{' to the first '}' after it
        assertEquals(Slots.crc16("foo{}{bar}".getBytes(StandardCharsets.US_ASCII), 0, 10) & 16383,
                slot("foo{}{bar}"));                           // an empty tag means the whole key is hashed
    }

    @Test
    void aSliceIsHashedLikeTheSameKeyOnItsOwn() {
        byte[] buffer = "xxGET foo{bar}yy".getBytes(StandardCharsets.US_ASCII);
        assertEquals(slot("foo{bar}"), Slots.slot(buffer, 6, 8));
    }
}
