/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent.output;

import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author roach
 */
public class ErlangTerm
{
    
    private final String command;
    private final String protocolName;
    private final List<String> data = new LinkedList<String>();
    private boolean success = true;
    private String errorMessage;
    
    
    public ErlangTerm(String command, String protocolName)
    {
        this.command = command;
        this.protocolName = protocolName;
        
    }
    
    public void failOperation(String errorMessage)
    {
        this.errorMessage = errorMessage;
        success = false;
    }
    
    public void addStringToResultData(String string)
    {
        data.add("\"" + string + "\"");
        
    }
    
    public void addByteArrayToResultData(byte[] array)
    {
        StringBuilder binary = new StringBuilder();
        binary.append("<<");
        for (byte b : array)
        {
            binary.append((int)b);
            binary.append(",");
        }
        binary.deleteCharAt(binary.length() - 1);
        binary.append(">>");
    
        data.add(binary.toString());
    
    }
    
    public void addKeyValuePairToResults(String key, Object value)
    {
        StringBuilder sb = new StringBuilder("{").append(key).append(",");
        if (value == null)
        {
            sb.append("null}");
        }
        else
        {
            if (value instanceof String)
            {
                sb.append("\"").append(value).append("\"}");
            }
            else
            {
                sb.append(value).append("}");
            }
        }
        
        data.add(sb.toString());
    }
    
    
    
     
    
    public void noResultData()
    {
        // no op
    }
    
    @Override
    public String toString()
    {
        StringBuilder term = new StringBuilder("{");
        
        term.append(command);
        term.append(",").append(protocolName);
        
        if (success)
        {
            term.append(",ok,[");
            
            if (!data.isEmpty())
            {
                for (String s : data)
                {
                    term.append(s).append(",");
                }
                
                term.deleteCharAt(term.length() - 1);
                
            }
            
            term.append("]}.");
            
        }
        else
        {
            term.append(",error,\"").append(errorMessage).append("\"}.");
        }
       
        return term.toString();
        
        
    }
    
    
}
