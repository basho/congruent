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
public class Delete extends RiakOperation 
{

    @Override
    public String execute() 
    {
        String bucketName = new String(Base64.decodeBase64(commandNode.get("bucket").getTextValue()));
        String key = new String(Base64.decodeBase64(commandNode.get("key").getTextValue()));
        
        /*
         * erlang term will look like:
         * { command, [{ protocol, { ok, [ data ]}}, ...]  }
         * { command, [{ protocol, {error, "string"}}, ... ] } 
         */ 
        
        
        ErlangTerm term = new ErlangTerm(commandNode.get("command").getTextValue());
        
        for (String name : riakClientMap.keySet())
        {
            IRiakClient client = riakClientMap.get(name);
       
            try 
            {
                Bucket bucket = client.fetchBucket(bucketName).execute();
                bucket.delete(key).execute();
                term.getProtoResult(name).noData();
            }
            catch (RiakException ex)
            {
                term.getProtoResult(name).fail(ex.getMessage());
            }
        }
        
        return term.toString();
    }
    
}
