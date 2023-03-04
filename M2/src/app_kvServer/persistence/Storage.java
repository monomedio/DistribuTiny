package app_kvServer.persistence;

import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.HashMap;

public class Storage {

    private String path;

    public Storage(String path) {
        this.path = path;
        initializeStorage();
    }
    public boolean inStorage(String key) {
        return new File(path + "/" + key).isFile();
    }

    public boolean clearStorage() {
        File directory = new File(path);
        boolean deleted = true;
        for (File f : directory.listFiles()) {
            deleted = deleted && f.delete();
        }
        return deleted;
    }

    public void initializeStorage() {
        File directory = new File(path);
        if (!directory.exists() || !directory.isDirectory()) {
            try {
                Files.createDirectory(directory.toPath());
            } catch(IOException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean put(String key, String value) {
        try {
            Files.write(FileSystems.getDefault().getPath(path, key), value.getBytes());
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public boolean delete(String key) {
        File f = new File(path + File.separator + key);
        return f.delete();
    }

    public String get(String key) throws IOException {
        return Files.readString(FileSystems.getDefault().getPath(path, key));
    }

    public boolean keyInRange(String key, String lowerRange, String upperRange) {
        String hashedKey = DigestUtils.md5Hex(key);
        if (hashedKey.compareTo(lowerRange) == 0) {
            return true;
        }
        // if lowerRange is larger than upperRange
        if (lowerRange.compareTo(upperRange) > 0) {
            // hashedkey <= lowerRange and hasedkey > upperRange
            return ((hashedKey.compareTo(lowerRange) <= 0) && hashedKey.compareTo(upperRange) > 0);
        } else {
            // lowerRange is smaller than upperRange (wrap around)
            return ((hashedKey.compareTo(lowerRange) <= 0 && (upperRange.compareTo(hashedKey) > 0))) ||
                    ((hashedKey.compareTo(lowerRange) > 0) && (upperRange.compareTo(hashedKey) < 0));
        }
    }

    public HashMap<String, String> createMap(String lowerRange, String upperRange) throws IOException {
        File folder = new File(path);
        HashMap<String, String> map = new HashMap<String, String>();
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory() || keyInRange(fileEntry.getName(), lowerRange, upperRange)) {
                System.out.println("Ignoring directory");
            } else {
                System.out.println(Files.readString(fileEntry.toPath()));
                map.put(fileEntry.getName(), Files.readString(fileEntry.toPath()));
            }
        }
        return map;
    }

    public HashMap<String, String> createMap() throws IOException {
        File folder = new File(path);
        HashMap<String, String> map = new HashMap<String, String>();
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                System.out.println("Ignoring directory");
            } else {
                System.out.println(Files.readString(fileEntry.toPath()));
                map.put(fileEntry.getName(), Files.readString(fileEntry.toPath()));
            }
        }
        return map;
    }

    public boolean processMap(String[] keyVals) {
        try {
            for (int i = 1; i < keyVals.length; i+=2) {
                Files.write(FileSystems.getDefault().getPath(path, keyVals[i - 1]), keyVals[i].getBytes());
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public boolean removeExtraData(String lowerRange, String upperRange) {
        File folder = new File(path);
        Boolean deleted = true;
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory() || keyInRange(fileEntry.getName(), lowerRange, upperRange)) {
                System.out.println("Ignoring directory");
            } else {
                System.out.println("Deleting " + fileEntry.getName());
                deleted = deleted && fileEntry.delete();
            }
        }
        return deleted;
    }
    // Used in testing
    public static void main(String[] args) {
    }
}
