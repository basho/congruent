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
public class CommandFactory 
{
    
    public RiakCommand createCommand(RawClient rawClient, JsonNode commandNode)
    {
        String commandName = commandNode.path("command").getTextValue();
        
        RiakCommand c = null;
        
        if (commandName.equalsIgnoreCase("ping"))
        {
            c = new Ping();
        }
        else if (commandName.equalsIgnoreCase("get"))
        {
            c = new Get();
        }
        else if (commandName.equalsIgnoreCase("put"))
        {
            c = new Put();
        }
        else if (commandName.equalsIgnoreCase("delete"))
        {
            c = new Delete();
        }
        else if (commandName.equalsIgnoreCase("keys"))
        {
            c = new Keys();
        }
        else
            c = new Unsupported();
        
        c.setClient(rawClient);
        c.setJson(commandNode);
        
        return c;
        
        
    }
    
    
}
