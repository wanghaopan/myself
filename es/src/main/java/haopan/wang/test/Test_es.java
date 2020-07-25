package haopan.wang.test;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import java.net.InetAddress;
import java.net.UnknownHostException;
public class Test_es {

    public static void main(String[] args) {
//        esClient("10.16.14.83",9300,"test.es0311","default_type_","1");
        esClient(args[0],Integer.valueOf(args[1]),args[2],args[3],args[4]);
//        esClient("10.168.25.189",31295,"test.es0311","default_type_","1");
    }

    public static void esClient(String IP,int PORT,String INDEX,String TYPE,String ID ){

        TransportClient client=null;
        try {

            Settings settings = Settings.builder()
//                    .put("client.transport.sniff", true)
                    .put("cluster.name", "elastic").build();
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName(IP), PORT));

            System.out.println("---------------------------------------------");
            System.out.println("cluster.name : "+"elastic");
            System.out.println("连接的IP："+ IP +"\n 连接的端口是："+ PORT);
            System.out.println("client : "+ client.toString());
            System.out.println("index: "+ INDEX + "\n type: "+ TYPE + "\n ID: " + ID);
            System.out.println("---------------------------------------------");


            GetResponse response = client.prepareGet(
                    INDEX, TYPE, ID).get();


            System.out.println("index: "+ response.getIndex());
            System.out.println("type: " + response.getType());
            System.out.println("ID: " + response.getId());
            System.out.println("JSON: " + response.getSourceAsString());


        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        finally {
            if (client!=null){
                client.close();
            }
        }
    }



}
