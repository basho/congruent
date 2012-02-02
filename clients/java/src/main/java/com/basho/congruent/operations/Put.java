/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent.operations;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.builders.RiakObjectBuilder;
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
public class Put extends RiakOperation
{
    @Override
    public String execute() 
    {
    
        String bucketName = new String(Base64.decodeBase64(commandNode.get("bucket").getTextValue()));
        String key = new String(Base64.decodeBase64(commandNode.get("key").getTextValue()));
        String value = new String(Base64.decodeBase64(commandNode.get("value").getTextValue()));
        
        
        try 
        {
            Bucket bucket = riakClient.fetchBucket(bucketName).execute();
            bucket.store(key,value);
            return "ok.";
        }
        catch (RiakRetryFailedException ex)
        {
            return "{error,\"" + ex.getMessage() + "\"}.";
        } 
        
        
    }    
}
