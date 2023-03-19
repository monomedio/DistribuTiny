package app_kvECS;

public interface IECS {

    /**
     * Starts running the server
     */
    public void run();

    /**
     * Gracefully stop the server, can perform any additional actions
     */
    public void close();
}
