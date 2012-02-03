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
        
        ErlangTerm term = new ErlangTerm(commandNode.get("command").getTextValue());
        
        for (String name : riakClientMap.keySet())
        {
            IRiakClient client = riakClientMap.get(name);
        
            try 
            {
                Bucket bucket = client.fetchBucket(bucketName).execute();

                Iterable<String> keys = bucket.keys();
                
                if (keys.iterator().hasNext())
                {
                    for (String k : keys)
                    {
                        term.getProtoResult(name).addString(k);
                    }
                }
                else
                {
                    term.getProtoResult(name).noData();
                }
            }
            catch (RiakException ex)
            {
                term.getProtoResult(name).fail(ex.getMessage());
            }
        }
        
        return term.toString();
        
    }
    
}
