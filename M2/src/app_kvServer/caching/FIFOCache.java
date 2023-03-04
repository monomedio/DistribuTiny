package app_kvServer.caching;

import java.util.HashMap;
import java.util.List;

public class FIFOCache implements Cache{

    private int cacheSize;

    private HashMap<String, String> cache;
    private int size;

    public FIFOCache(int cacheSize) {
        this.cacheSize = cacheSize;
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
