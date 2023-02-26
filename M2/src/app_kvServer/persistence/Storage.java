package app_kvServer.persistence;

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

    public static void main(String[] args) {
        Storage store = new Storage("sample_keys");
        System.out.println(store.put("hello", "bye"));
        try {
            store.createMap();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
