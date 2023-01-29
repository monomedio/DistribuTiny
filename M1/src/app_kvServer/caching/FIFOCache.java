package app_kvServer.caching;

import java.util.HashMap;
import java.util.List;

public class FIFOCache implements Cache{

    private HashMap<String, String> cache;
    private int size;

    public FIFOCache(int size) {

    }
    @Override
    public boolean inCache(String key) {
        return cache.containsKey(key);
    }

    @Override
    public String get(String key) {
        return null;
    }

    @Override
    public boolean put(String key, String val) {
        return false;
    }

    @Override
    public void clear() {

    }

    @Override
    public void evict() {

    }
}
