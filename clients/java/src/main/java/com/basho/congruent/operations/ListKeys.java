/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent.operations;

import com.basho.congruent.output.ErlangTerm;
import com.basho.riak.client.IRiakClient;
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
        
        ErlangTerm term = new ErlangTerm(commandName, protocolName);
        
        try 
        {
            Bucket bucket = client.fetchBucket(bucketName).execute();

            Iterable<String> keys = bucket.keys();

            if (keys.iterator().hasNext())
            {
                for (String k : keys)
                {
                    term.addStringToResultData(k);
                }
            }
            else
            {
                term.noResultData();
            }
        }
        catch (RiakException ex)
        {
            term.failOperation(ex.getMessage());
        }
        
        
        return term.toString();
        
    }
    
}
