/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent.operations;

import com.basho.congruent.output.ErlangTerm;
import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.operations.StoreObject;
import java.util.Iterator;
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
            StoreObject<IRiakObject> storeObject = bucket.store(key,value);

            Iterator<String> fieldIterator = commandNode.getFieldNames();
            
            
            while (fieldIterator.hasNext())
            {
                String field = fieldIterator.next();
                
                if (field.equalsIgnoreCase("w"))
                {
                    int i = commandNode.get(field).getValueAsInt(-1);
                    if (i == -1)
                    {
                        term.failOperation("symbolic quora not supported");
                        return term.toString();
                    }
                    else
                        storeObject.w(i);
                }
                else if (field.equalsIgnoreCase("dw"))
                {
                    int i = commandNode.get(field).getValueAsInt(-1);
                    if (i == -1)
                    {
                        term.failOperation("symbolic quora not supported");
                        return term.toString();
                    }
                    else
                        storeObject.dw(i);
                } 
                else if (field.equalsIgnoreCase("pw"))
                {
                    int i = commandNode.get(field).getValueAsInt(-1);
                    if (i == -1)
                    {
                        term.failOperation("symbolic quora not supported");
                        return term.toString();
                    }
                    else
                        storeObject.pw(i);
                }
                else if (field.equalsIgnoreCase("returnbody"))
                {
                    storeObject.returnBody(commandNode.get(field).getBooleanValue());
                }
                
            }
            
            IRiakObject riakObject = storeObject.execute();
            if (riakObject != null)
            {
                term.addByteArrayToResultData(riakObject.getValue());
            }
            else
                term.noResultData();
        }
        catch (RiakRetryFailedException ex)
        {
            term.failOperation(ex.getMessage());
        } 
        
        return term.toString();
    }    
}
