/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent.operations;

import com.basho.congruent.output.ErlangTerm;

/**
 *
 * @author roach
 */
public class Unsupported extends RiakOperation 
{

    @Override
    public String execute() 
    {
        ErlangTerm term = new ErlangTerm(commandName, protocolName);
        
        term.failOperation("unsupported");
        
        return term.toString();
        
    }
    
}
