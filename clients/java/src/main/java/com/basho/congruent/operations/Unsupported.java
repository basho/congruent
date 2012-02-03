/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent.operations;

import com.basho.congruent.output.ErlangTerm;
import com.basho.riak.client.IRiakClient;

/**
 *
 * @author roach
 */
public class Unsupported extends RiakOperation 
{

    @Override
    public String execute() 
    {
        ErlangTerm term = new ErlangTerm(commandNode.get("command").getTextValue());
        
        for (String name : riakClientMap.keySet())
        {
            term.getProtoResult(name).fail("unsupported");
        }
        
        return term.toString();
        
    }
    
}
