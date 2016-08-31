package cn.guoyukun.es;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.springframework.beans.factory.FactoryBean;

import java.net.InetAddress;

/**
 * Created by guoyukun on 2016/7/7.
 */
public class EsClientFactoryBean implements FactoryBean<Client> {

    private String clusterName;
    private String[] clusterNodes;

    @Override
    public Client getObject() throws Exception {
        //设置集群的名字
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", true)
                .build();

        //创建集群client并添加集群节点地址
        TransportClient client =TransportClient.builder().settings(settings).build();
        for (String node: clusterNodes){
            String[] hostPort = node.split(":");
            if(hostPort.length==2){
                String host = hostPort[0];
                int port = Integer.valueOf(hostPort[1]);
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),port));
            }else{
                throw new IllegalArgumentException("集群节点格式为host:port,host2:port2:"+node);
            }
        }

        return client;
    }

    @Override
    public Class<?> getObjectType() {
        return Client.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String[] getClusterNodes() {
        return clusterNodes;
    }

    public void setClusterNodes(String[] clusterNodes) {
        this.clusterNodes = clusterNodes;
    }
}
