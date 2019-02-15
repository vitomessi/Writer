/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package writer;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import java.io.IOException;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import util.Operation;

/**
 *
 * @author pmti9
 */
public class Writer {
    /*private static int writeQuorum = Integer.parseInt(System.getenv("WRITE_QUORUM"));
      private static String REPLICA_MANAGER_NAME = "replica-manager";
      public static int REPLICA_MANAGER_NUM = Integer.parseInt(System.getenv("REPLICA_MANAGER_NUM"));*/        
    //List<String> rm_urls = new ArrayList<String>();    
    private static final String REST_URI = "http://replica:8080/ReplicaManagerHomework2-web/webresources/operazioni";
    private static final String EXCHANGE_NAME = "logs";
    
    private static int writeQuorum = 1;
    private static int counterAck = 0; 
    //BUILD LIST OF REPLICA MANAGER URLS **/
        
        /*** 
         ** Urls will be like:
         **   - replica-manager-1
         **   - replica-manager-2
         **   - ....
         ***/
     /*
    
        
        for(int i = 0; i < 5; ++i ) {
            rm_urls.add(REPLICA_MANAGER_NAME + "-" + Integer.toString(i) );
    
        }*/
    
    //timer
    static int interval;
    static Timer timer;
    
    private static final int setInterval() {
	        if (interval == 1)
	            timer.cancel();
	        return --interval;
	    }
    
     /**
     * inizializza il timer 
     * @return 
     */
    public static  boolean timerAck(){
       
    int delay = 1000;
    int period = 1000;
    timer = new Timer();
    interval = 3;
    
    timer.scheduleAtFixedRate(new TimerTask() {
        @Override
            public void run() {
                if(setInterval() == 0)
                System.out.println("timer"); 
                //System.out.println("" + setInterval());           
                    }
                }, delay, period);
      return true;
    }
    

    public static void main(String[] args) {
        
        try {
            
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection connection;
            connection = factory.newConnection();
            connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, EXCHANGE_NAME, "");
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            JsonParser parser = new JsonParser();
            JsonObject jsonObject = parser.parse(message).getAsJsonObject();
                for (Map.Entry<String, JsonElement> e : jsonObject.entrySet()) {
                    
                    String name= e.getKey();
                    Object valueObject = jsonObject.get(e.getKey());
                    String value = String.valueOf(valueObject);
                    System.out.println("key " + name);
                    System.out.println("value " + value);
            
                    //invio dell'operazione al replica manager
                    Operation op = new Operation(name,value);
                    String json = new Gson().toJson(op);
                    Client client = Client.create();
                    /*for (int i = 0; i<WRITE_QUORUM; i++){
                      WebResource resource = client.resource("http://" + rm_urls[i] + ":8080/ReplicaManagerHomework2-web/webresources/operazioni").path("addLog");
                      ClientResponse response = resource.post(ClientResponse.class, json);
                    //aspetta ack dal replica Manager
                    if(response.getStatus()==200) {
                        counterAck++;
                    }
                    }*/
                    WebResource resource = client.resource(REST_URI).path("addLog");
                    ClientResponse response = resource.post(ClientResponse.class, json);
                    //aspetta ack dal replica Manager
                    if(response.getStatus()==200) {
                        counterAck++;
                    }
                    if(counterAck >= writeQuorum && timerAck() == true)
                    {
                        /*for (int i = 0; i<WRITE_QUORUM; i++){
                      WebResource resource = client.resource("http://" + rm_urls[i] + ":8080/ReplicaManagerHomework2-web/webresources/operazioni").path("add");
                      ClientResponse response = resource.post(ClientResponse.class, json);
                        */
                        WebResource post_resource = client.resource(REST_URI).path("add");
                        ClientResponse post_response = post_resource.post(ClientResponse.class, json);
                        
                    } else {
                          /*for (int i = 0; i<WRITE_QUORUM; i++){
                      WebResource resource = client.resource("http://" + rm_urls[i] + ":8080/ReplicaManagerHomework2-web/webresources/operazioni").path("sendAbort");
                      ClientResponse response = resource.post(ClientResponse.class, json);
                        */
                        WebResource resource_abort = client.resource(REST_URI).path("sendAbort");
                        ClientResponse response_abort = resource_abort.post(ClientResponse.class, json);
                    }
                    
                    System.out.println(json);
                    
                }
            };
            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
             
            }
            catch (Exception ex) {
                Logger.getLogger(Writer.class.getName()).log(Level.SEVERE, null, ex);
            }
        
    }
    
    
    
}
