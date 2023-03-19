package testing;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetadataAddTest {

    public List<Object> updateMetadataAdd(
            String hash, String ipAndPort, HashMap<String, String> metadata, HashMap<String, String> connections){
        String successor = null;


        if (metadata.isEmpty()) {
            metadata.put(ipAndPort, hash + "," + hash);
            //return null;
        } else {
            // Key whose lowerRange is the smallest
            String minKey = null;
            // Key whose lowerRange is the smallest that is larger than hash
            String targetKey = null;

            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                String entryKey = entry.getKey();
                String entryLower = entry.getValue().split(",")[0];

                // minKey computation
                if (minKey == null) {
                    minKey = entryKey;
                } else if (metadata.get(minKey).split(",")[0].compareTo(entryLower) > 0) { // if minKey's lowerRange is larger than entryLower
                    minKey = entryKey;
                }

                // targetKey computation
                if ((targetKey == null)) {
                    if (hash.compareTo(entryLower) < 0) {
                        targetKey = entryKey;
                    }
                } else {
                    if ((metadata.get(targetKey).split(",")[0].compareTo(entryLower) > 0) && (hash.compareTo(entryLower) < 0)) {
                        targetKey = entryKey;
                    }
                }
            }

            // minKey will be its successor
            if (targetKey == null) {
                String[] minLowerUpper = metadata.get(minKey).split(",");
                String minLower = minLowerUpper[0];
                String minUpper = minLowerUpper[1];
                metadata.put(minKey, minLower + "," + hash);
                metadata.put(ipAndPort, hash + "," + minUpper);
                successor = connections.get(minKey);
            } else { // targetKey is successor
                String[] targetLowerUpper = metadata.get(targetKey).split(",");
                String targetLower = targetLowerUpper[0];
                String targetUpper = targetLowerUpper[1];
                metadata.put(targetKey, targetLower + "," + hash);
                metadata.put(ipAndPort, hash + "," + targetUpper);
                successor = connections.get(targetKey);
            }


        }

        List<Object> result = new ArrayList<>();
        result.add(metadata);
        result.add(successor);
        return result;
    }

    @Test
    public void no_server_add_one(){
        // Case 1: no servers currently, add one (range should cover everything)
        String ipAndPort1 = "127.0.0.1:1";
        String hash1 = "8".repeat(32);

        HashMap<String, String> metadata1 = new HashMap<>();
        HashMap<String, String> connections1 = new HashMap<>();

        // The resulting medatadata1 should be
        HashMap<String, String> metadata1Result = new HashMap<>();
        metadata1Result.put(ipAndPort1, hash1 + "," + hash1);

        List<Object> result1 = updateMetadataAdd(hash1, ipAndPort1, metadata1, connections1);

        //assert(result1 == null);

        assert(result1.get(0).equals(metadata1Result)); // metadata check
        assert(result1.get(1) == null); // successor check
    }

    @Test
    public void one_server_add_less_serv1(){
        // Case 2: one server currently, add one < server1 (server1 upper should == server2 lower and vice versa)
        String ipAndPort2 = "127.0.0.1:2";
        String hash2 = "80e7bcff701a97e48834556f72689200";

        HashMap<String, String> metadata2 = new HashMap<>();
        HashMap<String, String> connections2 = new HashMap<>();

        String hash2_1 = "d".repeat(32);
        metadata2.put("127.0.0.1:1", hash2_1 + "," + hash2_1);
        connections2.put("127.0.0.1:1", "ECSComm 127.0.0.1:1");

        // The resulting medatadata2 should be
        HashMap<String, String> metadata2Result = new HashMap<>();
        metadata2Result.put("127.0.0.1:1", hash2_1 + "," + hash2);
        metadata2Result.put(ipAndPort2, hash2 + "," + hash2_1);

        List<Object> result2 = updateMetadataAdd(hash2, ipAndPort2, metadata2, connections2);

        assert(result2.get(0).equals(metadata2Result)); // metadata check
        assert(result2.get(1) == "ECSComm 127.0.0.1:1"); // successor check
    }

    @Test
    public void one_server_add_greater_serv1(){
        //Case 3: one server currently, add one > server1 (server1 upper should == server2 lower and vice versa)
        String ipAndPort3 = "127.0.0.1:2";
        String hash3 = "80e7bcff701a97e48834556f72689200";

        HashMap<String, String> metadata3 = new HashMap<>();
        HashMap<String, String> connections3 = new HashMap<>();

        String hash3_1 = "1".repeat(32);
        metadata3.put("127.0.0.1:1", hash3_1 + "," + hash3_1);
        connections3.put("127.0.0.1:1", "ECSComm 127.0.0.1:1");

        // The resulting medatadata3 should be
        HashMap<String, String> metadata3Result = new HashMap<>();
        metadata3Result.put("127.0.0.1:1", hash3_1 + "," + hash3);
        metadata3Result.put(ipAndPort3, hash3 + "," + hash3_1);

        List<Object> result3 = updateMetadataAdd(hash3, ipAndPort3, metadata3, connections3);

        assert(result3.get(0).equals(metadata3Result)); // metadata check
        assert(result3.get(1) == "ECSComm 127.0.0.1:1"); // successor check
    }

    @Test
    public void two_servers_add_w_serv1_succ(){
        // Case 4: two servers currently, add one with serv1 as successor, serv2 range stays the same
        String ipAndPort4 = "127.0.0.1:3";
        String hash4 = "80e7bcff701a97e48834556f72689200";

        HashMap<String, String> metadata4 = new HashMap<>();
        HashMap<String, String> connections4 = new HashMap<>();

        String hash4_1 = "80e7bcff701a97e48834556f72689308";
        String hash4_2 = "0".repeat(32);

        metadata4.put("127.0.0.1:1", hash4_1 + "," + hash4_2);
        connections4.put("127.0.0.1:1", "ECSComm 127.0.0.1:1");

        metadata4.put("127.0.0.1:2", hash4_2 + "," + hash4_1);
        connections4.put("127.0.0.1:2", "ECSComm 127.0.0.1:2");

        // The resulting medatadata4 should be
        HashMap<String, String> metadata4Result = new HashMap<>();
        metadata4Result.put("127.0.0.1:1", hash4_1 + "," + hash4);
        metadata4Result.put("127.0.0.1:2", hash4_2 + "," + hash4_1);
        metadata4Result.put(ipAndPort4, hash4 + "," + hash4_2);

        List<Object> result4 = updateMetadataAdd(hash4, ipAndPort4, metadata4, connections4);

        assert(result4.get(0).equals(metadata4Result)); // metadata check
        assert(result4.get(1) == "ECSComm 127.0.0.1:1"); // successor check

    }

    @Test
    public void two_servers_add_new_across_wraparound(){
        // Case 5: two servers currently, add one with serv1 as successor, serv2 range stays the same
        // new server at f*32 so successor crosses the wrap around
        String ipAndPort5 = "127.0.0.1:3";
        String hash5 = "f".repeat(32);

        HashMap<String, String> metadata5 = new HashMap<>();
        HashMap<String, String> connections5 = new HashMap<>();

        String hash5_1 = "1".repeat(32);
        String hash5_2 = "9".repeat(32);

        metadata5.put("127.0.0.1:1", hash5_1 + "," + hash5_2);
        connections5.put("127.0.0.1:1", "ECSComm 127.0.0.1:1");

        metadata5.put("127.0.0.1:2", hash5_2 + "," + hash5_1);
        connections5.put("127.0.0.1:2", "ECSComm 127.0.0.1:2");

        // The resulting metadata5 should be
        HashMap<String, String> metadata5Result = new HashMap<>();
        metadata5Result.put("127.0.0.1:1", hash5_1 + "," + hash5);
        metadata5Result.put("127.0.0.1:2", hash5_2 + "," + hash5_1);
        metadata5Result.put(ipAndPort5, hash5 + "," + hash5_2);

        List<Object> result5 = updateMetadataAdd(hash5, ipAndPort5, metadata5, connections5);

        assert(result5.get(0).equals(metadata5Result)); // metadata check
        assert(result5.get(1) == "ECSComm 127.0.0.1:1"); // successor check
    }

    @Test
    public void three_serves_add_one(){
        // Case 6: three servers currently, add one
        String ipAndPort6 = "127.0.0.1:4";
        String hash6 = "b".repeat(32);

        HashMap<String, String> metadata6 = new HashMap<>();
        HashMap<String, String> connections6 = new HashMap<>();

        String hash6_1 = "4".repeat(32);
        String hash6_2 = "8".repeat(32);
        String hash6_3 = "f".repeat(32);

        metadata6.put("127.0.0.1:1", hash6_1 + "," + hash6_3);
        connections6.put("127.0.0.1:1", "ECSComm 127.0.0.1:1");

        metadata6.put("127.0.0.1:2", hash6_2 + "," + hash6_1);
        connections6.put("127.0.0.1:2", "ECSComm 127.0.0.1:2");

        metadata6.put("127.0.0.1:3", hash6_3 + "," + hash6_2);
        connections6.put("127.0.0.1:3", "ECSComm 127.0.0.1:3");

        // The resulting metadata6 should be
        HashMap<String, String> metadata6Result = new HashMap<>();
        metadata6Result.put("127.0.0.1:1", hash6_1 + "," + hash6_3);
        metadata6Result.put("127.0.0.1:2", hash6_2 + "," + hash6_1);
        metadata6Result.put("127.0.0.1:3", hash6_3 + "," + hash6);
        metadata6Result.put(ipAndPort6, hash6 + "," + hash6_2);

        List<Object> result6 = updateMetadataAdd(hash6, ipAndPort6, metadata6, connections6);

        assert(result6.get(0).equals(metadata6Result)); // metadata check
        assert(result6.get(1) == "ECSComm 127.0.0.1:3"); // successor check
    }

}
