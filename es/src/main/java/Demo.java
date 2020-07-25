//import org.elasticsearch.action.get.GetResponse;
//import org.elasticsearch.action.index.IndexResponse;
//import org.elasticsearch.client.transport.TransportClient;
//import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;
//import org.elasticsearch.common.transport.TransportAddress;
//import org.elasticsearch.common.xcontent.XContentBuilder;
//import org.elasticsearch.common.xcontent.XContentFactory;
//import org.elasticsearch.transport.client.PreBuiltTransportClient;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.net.InetAddress;
//import java.net.UnknownHostException;
//
//public class Demo {
//    private String IP;
//    private int PORT;
//
//    @Before
//    public void init(){
//        this.IP = "10.168.25.189";
//        this.PORT = 31295;
//    }
//
//    @Test
//    public void esClient(){
//        try {
//            Settings settings = Settings.builder().put("cluster.name", "elastic").build();
//            TransportClient client = new PreBuiltTransportClient(settings)
//                    .addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName(IP), PORT));
//            System.out.println(client.toString());
//
//            GetResponse response = client.prepareGet(
//                    "es_test.es_innovate_wlw", "default_type_", "1")
//            .execute().actionGet();
////                    .get();
//            System.out.println(response.toString());
//
//
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//    }
//
//
//
//
//
//}
//
//
//
