import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
        String oldHazelCastURL = "XXXXX";
        HazelcastInstance oldhzClient = getHazelcastInstance(oldHazelCastURL);

        String newHazelCastURL = "XXXXXX2";
        HazelcastInstance newhzClient = getHazelcastInstance(newHazelCastURL);


        System.out.println("Before Copy");
        newhzClient.getDistributedObjects().stream().forEach(c -> c.destroy());
        System.out.println("Old Map Count " + oldhzClient.getDistributedObjects().stream().count());
        System.out.println("New Map Count " + newhzClient.getDistributedObjects().stream().count());

        try {
            Collection<DistributedObject> oldhzClientDistributedObjects = oldhzClient.getDistributedObjects();

            for (DistributedObject distributedObject : oldhzClientDistributedObjects) {
                IMap<Object, Object> existingValues = oldhzClient.getMap(distributedObject.getName());

                //write old map to new hazelcast instance
                MultiMap<Object, Object> newHzClientMap = newhzClient.getMultiMap(distributedObject.getName());

                for (Object object : existingValues.keySet()) {
                    existingValues.entrySet();
                    Object obj = existingValues.get(object);
                    newHzClientMap.put(object, obj);
                    System.out.println("Putting " + object);

                }
                //newHzClientMap.putAll(existingValues);
            }
            System.out.println("After Copy");
            System.out.println("Old Map Count " + oldhzClient.getDistributedObjects().stream().count());
            System.out.println("New Map Count " + newhzClient.getDistributedObjects().stream().count());
        } finally {
            System.out.println("Shutting Down Clients");
            oldhzClient.shutdown();
            newhzClient.shutdown();
        }
    }

    private static HazelcastInstance getHazelcastInstance(String hazelCastURL) {

        ClientConfig oldClientConfig = new ClientConfig();
        GroupConfig oldClientConfigGroupConfig = oldClientConfig.getGroupConfig();
        oldClientConfigGroupConfig.setName("dev");
        oldClientConfigGroupConfig.setPassword("dev-pass");
        ClientNetworkConfig oldClientNetworkConfig = new ClientNetworkConfig();
        oldClientNetworkConfig.setAddresses(Arrays.asList(hazelCastURL)); //ec2-54-154-203-95.eu-west-1.compute.amazonaws.com 172.31.11.15
        oldClientConfig.setNetworkConfig(oldClientNetworkConfig);
        SerializationConfig serializationConfig = new SerializationConfig();

        serializationConfig.setPortableVersion(0);
        serializationConfig.setAllowUnsafe(false);
        //serializationConfig.setUseNativeByteOrder(false);
        serializationConfig.setEnableCompression(false);
        //serializationConfig.set
        oldClientConfig.setSerializationConfig(serializationConfig);

        return HazelcastClient.newHazelcastClient(oldClientConfig);
    }

}
