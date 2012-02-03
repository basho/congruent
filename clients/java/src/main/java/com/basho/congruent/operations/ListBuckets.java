/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent.operations;

import com.basho.riak.client.RiakException;
import java.util.Set;

/**
 *
 * @author roach
 */
public class ListBuckets extends RiakOperation
{

    @Override
    public String execute()
    {
        try
        {
            Set<String> bucketList = riakClient.listBuckets();
            
            StringBuilder term = new StringBuilder("{ok,[");
            
            if (!bucketList.isEmpty())
            {
                for (String bucket : bucketList)
                {
                    term.append("\"").append(bucket).append("\"").append(",");
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
