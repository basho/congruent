/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent.operations;

import com.basho.congruent.output.ErlangTerm;
import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;


/**
 *
 * @author roach
 */
public class Ping extends RiakOperation 
{
    @Override
    public String execute() {
        
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
                client.ping();
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
