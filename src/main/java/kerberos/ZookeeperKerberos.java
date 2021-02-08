package kerberos;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;

import java.util.Properties;

public class ZookeeperKerberos {
    public static void main(String[] args) {
        System.setProperty("java.security.krb5.conf", "d://krb5.conf");
        System.setProperty("java.security.auth.login.config", "d://kafka_client_jaas.conf");

        ZkUtils zkUtils = ZkUtils.apply("localhost:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
        System.out.println(AdminUtils.topicExists(zkUtils, "test"));
        AdminUtils.createTopic(zkUtils, "test1", 1, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);
        zkUtils.close();
    }
}
