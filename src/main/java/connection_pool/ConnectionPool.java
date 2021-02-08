package connection_pool;

/**
 * 连接池接口
 *
 * @author felixzh
 */
public interface ConnectionPool<T> {
    /**
     * 初始化：连接池最大连接数、从连接池获取连接最大等待时间(ms)
     */
    void init(Integer maxActiveSize, Long maxWaitTimeMs);

    /**
     * 从连接池获取连接
     */
    T getConnection() throws Exception;

    /**
     * 释放连接，归还给连接池
     */
    void releaseConnection(T connection) throws Exception;

    /**
     * 关闭连接池
     */
    void close();
}
