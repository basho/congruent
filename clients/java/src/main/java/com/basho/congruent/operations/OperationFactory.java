/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent.operations;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.raw.RawClient;
import org.codehaus.jackson.JsonNode;

/**
 *
 * @author roach
 */
public class OperationFactory 
{
    
    private IRiakClient httpClient;  
    
    public OperationFactory(String baseUrl, int httpPort, int pbPort) throws RiakException
    {
        String clientString = "http://" + baseUrl + ":"
                + httpPort + "/riak";
        
        httpClient = RiakFactory.httpClient(clientString);
        
        
    }
    
    public RiakOperation createOperation(JsonNode commandNode)
    {
        String operationName = commandNode.path("command").getTextValue();
        
        RiakOperation c = null;
        
        if (operationName.equalsIgnoreCase("ping"))
        {
            c = new Ping();
        }
        else if (operationName.equalsIgnoreCase("get"))
        {
            c = new Get();
        }
        else if (operationName.equalsIgnoreCase("put"))
        {
            c = new Put();
        }
        else if (operationName.equalsIgnoreCase("delete"))
        {
            c = new Delete();
        }
        else if (operationName.equalsIgnoreCase("keys"))
        {
            c = new ListKeys();
        }
        else if (operationName.equalsIgnoreCase("listBuckets"))
        {
            c = new ListBuckets();
        }
        else
            c = new Unsupported();
        
        c.setClient(httpClient);
        c.setJson(commandNode);
        
        return c;
        
        
    }
    
    
}
