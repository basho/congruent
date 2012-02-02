/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent.operations;

import com.basho.riak.client.RiakException;
import com.basho.riak.client.bucket.Bucket;
import org.apache.commons.codec.binary.Base64;

/**
 *
 * @author roach
 */
public class ListKeys extends RiakOperation 
{

    @Override
    public String execute() 
    {
        String bucketName = new String(Base64.decodeBase64(commandNode.get("bucket").getTextValue()));
        
        try 
        {
            Bucket bucket = riakClient.fetchBucket(bucketName).execute();
            
            Iterable<String> keys = bucket.keys();
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
        catch (RiakException ex)
        {
            return "{error,\"" + ex.getMessage() + "\"}.";
        }
        
    }
    
}
