/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent.operations;

import com.basho.congruent.output.ErlangTerm;
import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
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
        
        ErlangTerm term = new ErlangTerm(commandName, protocolName);

        try 
        {
            Bucket bucket = client.fetchBucket(bucketName).execute();
            bucket.store(key,value).execute();
            term.noResultData();
        }
        catch (RiakRetryFailedException ex)
        {
            term.failOperation(ex.getMessage());
        } 
        
        return term.toString();
    }    
}
