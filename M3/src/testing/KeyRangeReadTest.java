package testing;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KeyRangeReadTest extends TestCase {

    public String metadataToStringRead(HashMap<String, String> metadata) {
        StringBuilder res = new StringBuilder();
        int num_entries = metadata.size();
        for (Map.Entry<String, String> entry : metadata.entrySet()) { // for every entry in the metadata, find their extended lower range
            String extendedLower = "";
            String mainLower = entry.getValue().split(",")[0];
            for (Map.Entry<String, String> entry1 : metadata.entrySet()) {
//                System.out.println(entry1.getValue().split(",")[1] + " comparing to " + mainLower);
                if (entry1.getValue().split(",")[1].compareTo(mainLower) == 0) {
//                    System.out.println("IF1 passed");
                    extendedLower = entry1.getValue().split(",")[0];
                    break;
                }
            }
            if (num_entries >= 3) {
                for (Map.Entry<String, String> entry2 : metadata.entrySet()) {
//                    System.out.println(entry2.getValue().split(",")[1] + " comparing to " + extendedLower);
                    if (entry2.getValue().split(",")[1].compareTo(extendedLower) == 0) {
//                        System.out.println("IF2 passed");
                        extendedLower = entry2.getValue().split(",")[0];
                        break;
                    }
                }
            }
            res.append(extendedLower);
            res.append(",");
            res.append(entry.getValue().split(",")[1]);
            res.append(",");
            res.append(entry.getKey());
            res.append(";");
        }

        res.deleteCharAt(res.length() - 1);
        return res.toString();
    }

    @Test
    public void test_one_server(){
        HashMap<String, String> metadata = new HashMap<>();
        metadata.put("127.0.0.1:8010", "cee1458b33a5f7bd0675d63d94ddd2cd,cee1458b33a5f7bd0675d63d94ddd2cd");
        String res = metadataToStringRead(metadata);
        assert(res.compareTo("cee1458b33a5f7bd0675d63d94ddd2cd,cee1458b33a5f7bd0675d63d94ddd2cd,127.0.0.1:8010") == 0);
    }

    @Test
    public void test_two_servers() {
        HashMap<String, String> metadata = new HashMap<>();
        metadata.put("127.0.0.1:8010", "8f2d5bc4bdd21ff5d5e7cafa3e3464d4,cee1458b33a5f7bd0675d63d94ddd2cd");
        metadata.put("127.0.0.1:8011", "cee1458b33a5f7bd0675d63d94ddd2cd,8f2d5bc4bdd21ff5d5e7cafa3e3464d4");
        String res = metadataToStringRead(metadata);
        assert(res.compareTo("cee1458b33a5f7bd0675d63d94ddd2cd,cee1458b33a5f7bd0675d63d94ddd2cd,127.0.0.1:8010;8f2d5bc4bdd21ff5d5e7cafa3e3464d4,8f2d5bc4bdd21ff5d5e7cafa3e3464d4,127.0.0.1:8011") == 0 ||
                res.compareTo("8f2d5bc4bdd21ff5d5e7cafa3e3464d4,8f2d5bc4bdd21ff5d5e7cafa3e3464d4,127.0.0.1:8011;cee1458b33a5f7bd0675d63d94ddd2cd,cee1458b33a5f7bd0675d63d94ddd2cd,127.0.0.1:8010") == 0);
    }

    @Test
    public void test_three_servers() {
        HashMap<String, String> metadata = new HashMap<>();
        metadata.put("127.0.0.1:8010", "8f2d5bc4bdd21ff5d5e7cafa3e3464d4,cee1458b33a5f7bd0675d63d94ddd2cd");
        metadata.put("127.0.0.1:8011", "fd195faed9caee9f46eef6cad47f33b8,8f2d5bc4bdd21ff5d5e7cafa3e3464d4");
        metadata.put("127.0.0.1:8012", "cee1458b33a5f7bd0675d63d94ddd2cd,fd195faed9caee9f46eef6cad47f33b8");
        String res = metadataToStringRead(metadata);
        assert(res.contains("cee1458b33a5f7bd0675d63d94ddd2cd,cee1458b33a5f7bd0675d63d94ddd2cd,127.0.0.1:8010") && res.contains("8f2d5bc4bdd21ff5d5e7cafa3e3464d4,8f2d5bc4bdd21ff5d5e7cafa3e3464d4,127.0.0.1:8011") && res.contains("fd195faed9caee9f46eef6cad47f33b8,fd195faed9caee9f46eef6cad47f33b8,127.0.0.1:8012"));
    }

    @Test
    public void four_servers() {
        HashMap<String, String> metadata = new HashMap<>();
        metadata.put("127.0.0.1:8010", "8f2d5bc4bdd21ff5d5e7cafa3e3464d4,cee1458b33a5f7bd0675d63d94ddd2cd");
        metadata.put("127.0.0.1:8011", "37e294db1331b433dfb112bfc4c410d8,8f2d5bc4bdd21ff5d5e7cafa3e3464d4");
        metadata.put("127.0.0.1:8012", "cee1458b33a5f7bd0675d63d94ddd2cd,fd195faed9caee9f46eef6cad47f33b8");
        metadata.put("127.0.0.1:8013", "fd195faed9caee9f46eef6cad47f33b8,37e294db1331b433dfb112bfc4c410d8");
        String res = metadataToStringRead(metadata);
        assert(res.contains("37e294db1331b433dfb112bfc4c410d8,fd195faed9caee9f46eef6cad47f33b8,127.0.0.1:8012") &&
                res.contains("fd195faed9caee9f46eef6cad47f33b8,cee1458b33a5f7bd0675d63d94ddd2cd,127.0.0.1:8010") &&
                res.contains("cee1458b33a5f7bd0675d63d94ddd2cd,8f2d5bc4bdd21ff5d5e7cafa3e3464d4,127.0.0.1:8011") &&
                res.contains("8f2d5bc4bdd21ff5d5e7cafa3e3464d4,37e294db1331b433dfb112bfc4c410d8,127.0.0.1:8013"));
    }
}
