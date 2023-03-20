package testing;

import junit.framework.TestCase;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;

import java.util.*;
import java.util.ArrayList;

public class MetadataRemoveTest {
    public List<Object> updateMetadataRemove(String ipAndPort, HashMap<String, String> metadata) {
        String[] myRange = metadata.get(ipAndPort).split(",");
        String myToHash = myRange[0];
        String myFromHash = myRange[1];
        metadata.remove(ipAndPort);
        String fromHash = "";
        String toHash = "";
        String entryIp = "";
        for (Map.Entry<String, String> entry: metadata.entrySet()) {
            String[] range = entry.getValue().split(",");
            toHash = range[0];
            fromHash = range[1];
            entryIp = entry.getKey();
            if (toHash.equals(myFromHash)) {
                toHash = myToHash;
                break;
            }
        }

        metadata.replace(entryIp, toHash + "," + fromHash);
        //return this.connections.get(entryIp);

        List<Object> result = new ArrayList<>();
        result.add(metadata);
        result.add(entryIp);
        return result;
    }

    @Test
    public void one_server_remove(){
        HashMap<String, String> metadata = new HashMap<>();
        HashMap<String, String> connections = new HashMap<>();

        String hash1 = "d".repeat(32);
        metadata.put("127.0.0.1:1", hash1 + "," + hash1);
        connections.put("127.0.0.1:1", "ECSComm 127.0.0.1:1");

        // The resulting metadata should be
        HashMap<String, String> metadataResult = new HashMap<>();

        List<Object> result = updateMetadataRemove("127.0.0.1:1", metadata);

        assert(result.get(0).equals(metadataResult));
        assert(result.get(1) == "");

        // IS CONNECTIONS UPDATED ???

    }


    @Test
    public void two_servers_remove_one(){
        HashMap<String, String> metadata = new HashMap<>();
        HashMap<String, String> connections = new HashMap<>();

        String hash1 = "80e7bcff701a97e48834556f72689308";
        String hash2 = "0".repeat(32);

        metadata.put("127.0.0.1:1", hash2 + "," + hash1);
        connections.put("127.0.0.1:1", "ECSComm 127.0.0.1:1");

        metadata.put("127.0.0.1:2", hash1 + "," + hash2);
        connections.put("127.0.0.1:2", "ECSComm 127.0.0.1:2");

        // The resulting metadata should be
        HashMap<String, String> metadataResult = new HashMap<>();
        metadataResult.put("127.0.0.1:2", hash2 + "," + hash2);

        List<Object> result = updateMetadataRemove("127.0.0.1:1", metadata);

        assert(result.get(0).equals(metadataResult));
        assert(result.get(1) == "127.0.0.1:2");


    }

    @Test
    public void four_servers_remove_three(){
        HashMap<String, String> metadata = new HashMap<>();
        HashMap<String, String> connections = new HashMap<>();

        String hash1 = "4".repeat(32);
        String hash2 = "8".repeat(32);
        String hash3 = "f".repeat(32);
        String hash4 = "b".repeat(32);

        metadata.put("127.0.0.1:1", hash3 + "," + hash1);
        connections.put("127.0.0.1:1", "ECSComm 127.0.0.1:1");

        metadata.put("127.0.0.1:2", hash1 + "," + hash2);
        connections.put("127.0.0.1:2", "ECSComm 127.0.0.1:2");

        metadata.put("127.0.0.1:3", hash4 + "," + hash3);
        connections.put("127.0.0.1:3", "ECSComm 127.0.0.1:3");

        metadata.put("127.0.0.1:4", hash2 + "," + hash4);
        connections.put("127.0.0.1:4", "ECSComm 127.0.0.1:4");

        // The resulting metadata (after remove #1) should be
        HashMap<String, String> metadataResult1 = new HashMap<>();
        metadataResult1.put("127.0.0.1:1", hash3 + "," + hash1);
        metadataResult1.put("127.0.0.1:2", hash1 + "," + hash2);
        metadataResult1.put("127.0.0.1:3", hash2 + "," + hash3);

        List<Object> result1 = updateMetadataRemove("127.0.0.1:4", metadata);

        metadata = (HashMap<String, String>) result1.get(0);

        assert(result1.get(0).equals(metadataResult1));
        assert(result1.get(1).equals("127.0.0.1:3"));

        // The resulting metadata (after remove #2) should be
        HashMap<String, String> metadataResult2 = new HashMap<>();
        metadataResult2.put("127.0.0.1:1", hash3 + "," + hash1);
        metadataResult2.put("127.0.0.1:3", hash1 + "," + hash3);

        List<Object> result2 = updateMetadataRemove("127.0.0.1:2", metadata);

        assert(result2.get(0).equals(metadataResult2));
        assert(result2.get(1).equals("127.0.0.1:3"));
    }


}
