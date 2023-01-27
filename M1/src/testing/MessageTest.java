package testing;

import junit.framework.TestCase;
import org.junit.Test;


import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;
import shared.messages.KVMessage;

public class MessageTest extends TestCase{

    private IKVMessage kvMessage;

    @Test
    public void testThreeAryConstructor() {
        kvMessage = new KVMessage(StatusType.GET, "key", "value");
        assertTrue(kvMessage.getMessage().equals("GET key value")
                && kvMessage.getStatus().equals(StatusType.GET)
                && kvMessage.getKey().equals("key")
                && kvMessage.getValue().equals("value"));
    }
}
