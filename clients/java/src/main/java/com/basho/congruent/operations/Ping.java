/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent.operations;

import com.basho.congruent.output.ErlangTerm;
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
        
        ErlangTerm term = new ErlangTerm(commandName, protocolName);
                
        try
        {
            client.ping();
            term.noResultData();
        }
        catch (RiakException ex)
        {
            term.failOperation(ex.getMessage());
        }
        
        return term.toString();
        
    }
    
}
