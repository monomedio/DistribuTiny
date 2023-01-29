package app_kvServer.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;

public class Storage {

    private String path;

    public Storage(String path) {
        this.path = path;
        initializeStorage();
    }
    public boolean inStorage(String key) {
        return new File(path + "/" + key).isFile();
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

    public String get(String key) throws IOException {
        return Files.readString(FileSystems.getDefault().getPath(path, key));
    }

    public static void main(String[] args) {
        Storage store = new Storage("sample_keys");
        System.out.println(store.put("hello", "bye"));
    }
}
