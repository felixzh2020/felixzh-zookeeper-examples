package connection_pool;

import org.apache.zookeeper.ZooKeeper;


public class ZKConnectionPoolDemo {
    public static void main(String[] args) throws Exception {
        ZKConnectionPool pool = new ZKConnectionPool("felixzh:2181", 10, 10000L);

        while (true) {
            Thread.sleep(1000);
            new Thread(() -> {
                try {
                    ZooKeeper zooKeeper = pool.getConnection();
                    //模拟业务处理时间
                    Thread.sleep(10000);
                    //pool.releaseConnection(zooKeeper);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }).start();
        }
    }
}
