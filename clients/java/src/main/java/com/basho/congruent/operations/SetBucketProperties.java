/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent.operations;

import com.basho.congruent.output.ErlangTerm;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.WriteBucket;
import java.util.Iterator;
import org.apache.commons.codec.binary.Base64;

/**
 *
 * @author roach
 */
public class SetBucketProperties extends RiakOperation
{

    @Override
    public String execute()
    {
        
        
        String bucketName = new String(Base64.decodeBase64(commandNode.get("bucket").getTextValue()));
        ErlangTerm term = new ErlangTerm(commandName, protocolName);
        
        WriteBucket bucket = client.createBucket(bucketName);
        
        Iterator<String> fieldIterator = commandNode.getFieldNames();
        
        while (fieldIterator.hasNext())
        {
            String field = fieldIterator.next();
            
            if (field.equalsIgnoreCase("n_val"))
                bucket.nVal(commandNode.get(field).getIntValue());
            else if (field.equalsIgnoreCase("allow_multi"))
                bucket.allowSiblings(commandNode.get(field).getBooleanValue());
            else if (field.equalsIgnoreCase("last_write_wins"))
                bucket.lastWriteWins(commandNode.get(field).getBooleanValue());
            else if (field.equalsIgnoreCase("r"))
            {
                int i = commandNode.get("r").getValueAsInt(-1);
                if (i == -1)
                {
                    term.failOperation("symbolic quora not supported");
                    return term.toString();
                }
                else
                    bucket.r(i);
            }
            else if (field.equalsIgnoreCase("w"))
            {
                int i = commandNode.get("w").getValueAsInt(-1);
                if (i == -1)
                {
                    term.failOperation("symbolic quora not supported");
                    return term.toString();
                }
                else
                    bucket.w(i);
            }
            else if (field.equalsIgnoreCase("dw"))
            {
                int i = commandNode.get("dw").getValueAsInt(-1);
                if (i == -1)
                {
                    term.failOperation("symbolic quora not supported");
                    return term.toString();
                }
                else
                    bucket.dw(i);
            }
            else if (field.equalsIgnoreCase("rw"))
            {
                int i = commandNode.get("rw").getValueAsInt(-1);
                if (i == -1)
                {
                    term.failOperation("symbolic quora not supported");
                    return term.toString();
                }
                else
                    bucket.rw(i);
            }
            else if (field.equalsIgnoreCase("pr"))
            {
                int i = commandNode.get("pr").getValueAsInt(-1);
                if (i == -1)
                {
                    term.failOperation("symbolic quora not supported");
                    return term.toString();
                }
                else
                    bucket.pr(i);
            }
            else if (field.equalsIgnoreCase("pw"))
            {
                int i = commandNode.get("pw").getValueAsInt(-1);
                if (i == -1)
                {
                    term.failOperation("symbolic quora not supported");
                    return term.toString();
                }
                else
                    bucket.pw(i);
            }
            else if (field.equalsIgnoreCase("basic_quorum"))
            {
                bucket.basicQuorum(commandNode.get(field).getBooleanValue());
            }
        }
        
        try
        {
            bucket.execute();
            term.noResultData();
        }
        catch (RiakRetryFailedException ex)
        {
            term.failOperation(ex.getMessage());
        }
        
        return term.toString();
        
        
        
        
    }
    
}
