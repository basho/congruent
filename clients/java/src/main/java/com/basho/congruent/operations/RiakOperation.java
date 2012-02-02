/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent.operations;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakRetryFailedException;
import org.codehaus.jackson.JsonNode;

/**
 *
 * @author roach
 */
public abstract class RiakOperation 
{
    
    protected IRiakClient riakClient;
    protected JsonNode commandNode;
    
    public void setClient(IRiakClient rawClient)
    {
        this.riakClient = rawClient;
    }
    
    public void setJson(JsonNode commandNode)
    {
        this.commandNode = commandNode;
    }
    
    public abstract String execute();
    
}
