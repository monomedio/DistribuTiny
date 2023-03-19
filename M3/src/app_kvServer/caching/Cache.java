package app_kvServer.caching;

public interface Cache {

    public boolean inCache(String key);

    public String get(String key);

    public boolean put(String key, String val);

    public void clear();

    public void evict();
}
