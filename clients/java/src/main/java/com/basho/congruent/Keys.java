/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent;

import com.basho.riak.client.http.response.RiakIORuntimeException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.codec.binary.Base64;

/**
 *
 * @author roach
 */
public class Keys extends RiakCommand 
{

    @Override
    public String execute() 
    {
        String bucket = new String(Base64.decodeBase64(commandNode.get("bucket").getTextValue()));
        
        try 
        {
            Iterable<String> keys = rawClient.listKeys(bucket);
            StringBuilder term = new StringBuilder("{ok,[");
            
            if (keys.iterator().hasNext())
            {
                for (String k : keys)
                {
                    term.append("\"").append(k).append("\"").append(",");
                }
                
                term.deleteCharAt(term.length() - 1);
            }
            
            term.append("]}.");
            return term.toString();
            
        } 
        catch (IOException ex) 
        {
            return "{error,\"" + ex.getMessage() + "\"}.";
        }
        catch (RiakIORuntimeException ex)
        {
            return "{error,\"" + ex.getMessage() + "\"}.";
        }
    }
    
}
