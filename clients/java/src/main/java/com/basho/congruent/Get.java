/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.http.response.RiakIORuntimeException;
import com.basho.riak.client.raw.RiakResponse;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.codec.binary.Base64;

/**
 *
 * @author roach
 */
public class Get extends RiakCommand {

    @Override
    public String execute() 
    {
    
        String bucket = new String(Base64.decodeBase64(commandNode.get("bucket").getTextValue()));
        String key = new String(Base64.decodeBase64(commandNode.get("key").getTextValue()));
        try 
        {
            RiakResponse response = rawClient.fetch(bucket, key);
            StringBuilder term = new StringBuilder("{ok,");
            if (RiakResponse.empty().equals(response))
            {
                term.append("not_found}.");
            }
            else
            {
                term.append("[");
                IRiakObject[] rObjects = response.getRiakObjects();
                if (rObjects.length > 0)
                {
                    for ( IRiakObject rObj : rObjects) 
                    {
                        term.append("<<");
                        for (byte b : rObj.getValue())
                        {
                            term.append((int)b);
                            term.append(",");
                        }
                        term.deleteCharAt(term.length() - 1);
                        term.append(">>,");

                    }   
                    
                    term.deleteCharAt(term.length() - 1);
                }
                               
                term.append("]}.");
            }
            
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
