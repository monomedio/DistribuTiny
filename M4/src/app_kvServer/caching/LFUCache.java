package app_kvServer.caching;

import java.util.HashMap;

public class LFUCache implements Cache{

    private int cacheSize;

    private HashMap<String, String> cache;
    private HashMap<String, Integer> frequency;

    public LFUCache(int cacheSize) {
        this.cacheSize = cacheSize;
//        this.cache = HashMap.newHashMap(cacheSize);
    }
    @Override
    public boolean inCache(String key) {
        return cache.containsKey(key);
    }

    @Override
    public String get(String key) {
        frequency.replace(key, frequency.get(key) + 1);
        return cache.get(key);
    }

    @Override
    public boolean put(String key, String val) {
        if(cache.containsKey(key))
        {
            frequency.replace(key, frequency.get(key));
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

    }

    @Override
    public void evict() {

    }
}
