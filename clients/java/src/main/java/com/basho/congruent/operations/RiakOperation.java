/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent.operations;

import com.basho.riak.client.IRiakClient;
import java.util.HashMap;
import java.util.Map;
import org.codehaus.jackson.JsonNode;

/**
 *
 * @author roach
 */
public abstract class RiakOperation 
{
    
    
    protected Map<String, IRiakClient> riakClientMap;
    
    protected JsonNode commandNode;
    
    public void setClientMap(Map<String,IRiakClient> cMap)
    {
        riakClientMap = cMap;
    }
    
    public void addClient(String name, IRiakClient client)
    {
        riakClientMap.put(name, client);
    }
    
    public void setJson(JsonNode commandNode)
    {
        this.commandNode = commandNode;
    }
    
    public abstract String execute();
    
}
