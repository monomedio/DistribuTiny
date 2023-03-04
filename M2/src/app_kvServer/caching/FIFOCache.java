package app_kvServer.caching;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FIFOCache implements Cache{

    private int cacheSize;

    private LinkedHashMap<String, String> cache;

    public FIFOCache(int cacheSize) {
        this.cacheSize = cacheSize;
        this.cache = LinkedHashMap.newLinkedHashMap(cacheSize);
    }
    @Override
    public boolean inCache(String key) {
        return cache.containsKey(key);
    }

    @Override
    public String get(String key) {
        return cache.get(key);
    }

    @Override
    public boolean put(String key, String val) {
        if(cache.containsKey(key))
        {
            cache.replace(key, val);
            return true;
        }
        else if(cache.size() == this.cacheSize)
        {
            evict();
        }
        cache.put(key,val);
        return true;
    }

    @Override
    public void clear() {
        this.cache = LinkedHashMap.newLinkedHashMap(cacheSize);
    }

    @Override
    public void evict() {
        Map.Entry<String, String> entry = cache.entrySet().iterator().next();
        cache.remove(entry.getKey());
    }
}
