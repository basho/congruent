/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent.operations;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jackson.JsonNode;

/**
 *
 * @author roach
 */
public class OperationFactory 
{
    
    //private IRiakClient httpClient;  
    
    private static final Map<String, IRiakClient> clientMap =
        new HashMap<String, IRiakClient>();
    
    private static final Map<String, Class<? extends RiakOperation>> operations = 
        new HashMap<String, Class<? extends RiakOperation>>();
    
    static
    {
        operations.put("ping", Ping.class);
        operations.put("get", Get.class);
        operations.put("put", Put.class);
        operations.put("delete", Delete.class);
        operations.put("keys", ListKeys.class);
        operations.put("list_buckets", ListBuckets.class);
        operations.put("get_bucket_properties", GetBucketProperties.class);
    }
    
    
    public OperationFactory(String baseUrl, int httpPort, int pbPort) throws RiakException
    {
        String clientString = "http://" + baseUrl + ":"
                + httpPort + "/riak";
        
        //httpClient = RiakFactory.httpClient(clientString);
        clientMap.put("http", RiakFactory.httpClient(clientString));
        clientMap.put("pb", RiakFactory.pbcClient());
        
    }
    
    public RiakOperation createOperation(JsonNode commandNode)
    {
        String operationName = 
            commandNode.path("command").getTextValue().toLowerCase(Locale.US);
        
        IRiakClient client = 
            clientMap.get(commandNode.path("proto").getTextValue().toLowerCase(Locale.US));
        
        
        RiakOperation c = null;
        
        Class<? extends RiakOperation> clazz = 
            operations.get(operationName);
        
        if (clazz != null && client != null)
        {
            try
            {
                c = clazz.newInstance();
            }
            catch (InstantiationException ex)
            {
                Logger.getLogger(OperationFactory.class.getName()).log(Level.SEVERE, null, ex);
            }
            catch (IllegalAccessException ex)
            {
                Logger.getLogger(OperationFactory.class.getName()).log(Level.SEVERE, null, ex);
            }
        
            c.setClient(client);
            
        
        }
        else
        {
           c = new Unsupported(); 
        }
        
        c.setJson(commandNode);
        
        return c;
        
        
    }
    
    
}
