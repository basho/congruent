/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent.operations;

import com.basho.congruent.output.ErlangTerm;
import com.basho.riak.client.IRiakClient;
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
        
        ErlangTerm term = new ErlangTerm(commandNode.get("command").getTextValue());
        
        for (String name : riakClientMap.keySet())
        {
            IRiakClient client = riakClientMap.get(name);
        
            try
            {
                Set<String> bucketList = client.listBuckets();

                if (!bucketList.isEmpty())
                {
                    for (String bucket : bucketList)
                    {
                        term.getProtoResult(name).addString(bucket);
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
