package app_kvServer.caching;

public class LFUCache implements Cache{
    @Override
    public boolean inCache(String key) {
        return false;
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
