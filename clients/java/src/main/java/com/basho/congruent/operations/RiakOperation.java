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
    
    protected IRiakClient riakClient;
    protected final Map<String, IRiakClient> riakClientMap = 
        new HashMap<String, IRiakClient>();
    
    protected JsonNode commandNode;
    
    public void setClient(IRiakClient rawClient)
    {
        this.riakClient = rawClient;
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
