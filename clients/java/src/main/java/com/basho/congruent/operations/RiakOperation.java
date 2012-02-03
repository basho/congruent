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
    
    
    protected IRiakClient client;
    protected String commandName;
    protected String protocolName;
    
    
    protected JsonNode commandNode;
    
    public void setClient(IRiakClient client)
    {
        this.client = client;
    }
    
    public void setJson(JsonNode commandNode)
    {
        this.commandNode = commandNode;
        
        // Convenience as all the commands need these
        this.commandName = 
            commandNode.get("command").getTextValue();
        this.protocolName =
            commandNode.get("proto").getTextValue();
    }
    
    public abstract String execute();
    
}
