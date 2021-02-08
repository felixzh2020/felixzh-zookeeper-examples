package connection_pool;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * zookeeper连接池实现类
 *
 * @author felixzh
 */
public class ZKConnectionPool implements ConnectionPool<ZooKeeper> {

    private final Logger logger = LoggerFactory.getLogger(ZKConnectionPool.class);

    //zookeeper连接字符串,如：10.10.10.1:2181,10.10.10.2:2181
    private String zkIpPort;

    //连接池最大连接数
    private Integer maxActiveSize;
    //从连接池获取连接的最大等待时间
    private Long maxWaitTimeMs;
    //连接池活动连接数
    private AtomicInteger activeSize = new AtomicInteger(0);

    //连接池关闭标记
    private AtomicBoolean isClosed = new AtomicBoolean(false);

    //空闲队列
    private LinkedBlockingQueue<ZooKeeper> idleQueue = new LinkedBlockingQueue<>();
    //工作队列
    private LinkedBlockingQueue<ZooKeeper> busyQueue = new LinkedBlockingQueue<>();

    //等待连接建立的计数器
    private static ThreadLocal<CountDownLatch> countDownLatchThreadLocal = ThreadLocal.withInitial(() -> new CountDownLatch(1));

    public ZKConnectionPool(String zkIpPort, Integer maxActiveSize, Long maxWaitTimeMs) {
        this.zkIpPort = zkIpPort;
        init(maxActiveSize, maxWaitTimeMs);
    }

    @Override
    public void init(Integer maxActiveSize, Long maxWaitTimeMs) {
        this.maxActiveSize = maxActiveSize;
        this.maxWaitTimeMs = maxWaitTimeMs;
    }

    @Override
    public ZooKeeper getConnection() throws Exception {
        printQueueSize("getConnection");
        Long startTimeMillis = System.currentTimeMillis();
        ZooKeeper zooKeeper = idleQueue.poll();
        if (zooKeeper == null) {
            final CountDownLatch countDownLatch = countDownLatchThreadLocal.get();
            //判断连接是否已满
            if (activeSize.get() < maxActiveSize) {
                if (activeSize.incrementAndGet() <= maxActiveSize) {
                    //创建zookeeper连接
                    zooKeeper = new ZooKeeper(zkIpPort, 5000, (watchedEvent -> {
                        if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
                            countDownLatch.countDown();
                        } else {
                            logger.error(String.format("connect to zookeeper: %s fail, reason: %s", zkIpPort, watchedEvent.getState()));
                        }
                    }));
                    //等待连接建立完成
                    countDownLatch.await();
                    busyQueue.offer(zooKeeper);
                    return zooKeeper;
                } else {
                    activeSize.decrementAndGet();
                }
            }

            //如果没有可用连接，指定wait timeout重新获取一次
            try {
                long actualWaitTimeMs = maxWaitTimeMs - (System.currentTimeMillis() - startTimeMillis);
                zooKeeper = idleQueue.poll(actualWaitTimeMs, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                logger.warn(String.format("wait timeout fail: %s", e.getMessage()), e);
                throw new Exception(String.format("wait timeout fail: %s", e.getMessage()), e);
            }

            //判断耗时指定maxWaitTimeMs后，两次获取连接是否成功
            if (zooKeeper != null) {
                busyQueue.offer(zooKeeper);
                return zooKeeper;
            } else {
                logger.warn("get connection timeout, please retry");
                throw new Exception("get connection timeout, please retry");
            }
        }

        //空闲队列存在可用的连接，直接返回
        busyQueue.offer(zooKeeper);
        return zooKeeper;
    }

    @Override
    public void releaseConnection(ZooKeeper connection) throws Exception {
        printQueueSize("releaseConnection");
        if (connection == null) {
            return;
        }

        if (busyQueue.remove(connection)) {
            idleQueue.offer(connection);
        } else {
            activeSize.decrementAndGet();
            logger.error(String.format("activeSize: %s, busyQueueSize: %s, ideaQueueSize: %s", activeSize, busyQueue.size(), idleQueue.size()));
            throw new Exception("release connection fail");
        }
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            zookeeperClose(idleQueue);
            zookeeperClose(busyQueue);
        }
    }

    private void zookeeperClose(LinkedBlockingQueue<ZooKeeper> queue) {
        queue.forEach(zooKeeper -> {
            try {
                zooKeeper.close();
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
            }
        });
    }

    private void printQueueSize(String position) {
        logger.info(String.format("%s ==== busyQueue size:%s", position, busyQueue.size()));
        logger.info(String.format("%s ==== idleQueue size:%s", position, idleQueue.size()));
    }
}
