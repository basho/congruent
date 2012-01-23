/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent;

import com.basho.riak.client.raw.RawClient;
import org.codehaus.jackson.JsonNode;

/**
 *
 * @author roach
 */
public abstract class RiakCommand 
{
    
    protected RawClient rawClient;
    protected JsonNode commandNode;
    
    public void setClient(RawClient rawClient)
    {
        this.rawClient = rawClient;
    }
    
    public void setJson(JsonNode commandNode)
    {
        this.commandNode = commandNode;
    }
    
    public abstract String execute();
    
}
