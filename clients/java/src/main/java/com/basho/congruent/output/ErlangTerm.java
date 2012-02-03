/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent.output;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author roach
 */
public class ErlangTerm
{
    
    private Map<String, ProtocolResult> protoResultMap =
        new HashMap<String, ProtocolResult>();
    
    private String command;
    
    
    public ErlangTerm(String command)
    {
        this.command = command;
    }
    
    public ProtocolResult getProtoResult(String proto)
    {
        ProtocolResult pResult = protoResultMap.get(proto);
        if (pResult == null)
        {
            pResult = new ProtocolResult(proto);
            protoResultMap.put(proto, pResult);
        }
        
        return pResult;
        
    }
    
    @Override
    public String toString()
    {
        StringBuilder term = new StringBuilder("{");
        
        term.append(command);
        term.append(",[");
        
        Set<String> protos = protoResultMap.keySet();
        
        if (!protos.isEmpty())
        {
            for (String proto : protoResultMap.keySet())
            {
                ProtocolResult pr = protoResultMap.get(proto);
                term.append(pr.toString()).append(",");
            }
        
            term.deleteCharAt(term.length() -1);
        }
        
        term.append("]}.");
        
        return term.toString();
        
        
    }
    
    
}
