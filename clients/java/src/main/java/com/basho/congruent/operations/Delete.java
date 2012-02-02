/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent.operations;

import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.http.response.RiakIORuntimeException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.codec.binary.Base64;

/**
 *
 * @author roach
 */
public class Delete extends RiakOperation 
{

    @Override
    public String execute() 
    {
        String bucketName = new String(Base64.decodeBase64(commandNode.get("bucket").getTextValue()));
        String key = new String(Base64.decodeBase64(commandNode.get("key").getTextValue()));
        try 
        {
            Bucket bucket = riakClient.fetchBucket(bucketName).execute();
            bucket.delete(key).execute();
            return "ok.";
        }
        catch (RiakException ex)
        {
            return "{error,\"" + ex.getMessage() + "\"}.";
        }
        
    }
    
}
