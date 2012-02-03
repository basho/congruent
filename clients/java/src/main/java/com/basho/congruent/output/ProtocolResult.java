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
public class ProtocolResult
{
    private boolean success = true;
    private List<String> data = new LinkedList<String>();
    private String errorMessage;
    private String protocol;
    
    public ProtocolResult(String protocol)
    {
        this.protocol = protocol;
    }
     
    public void fail(String errorMessage)
    {
        this.errorMessage = errorMessage;
        success = false;
    }
    
    public void addString(String string)
    {
        data.add("\"" + string + "\"");
        
    }
    
    public void noData()
    {
        // no op
    }
    
    public void addByteArray(byte[] array)
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
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("{");
        sb.append(protocol).append(",{");
        
        if (success)
        {
            sb.append("ok,[");
            
            if (!data.isEmpty())
            {
                for (String s : data)
                {
                    sb.append(s).append(",");
                }
                
                sb.deleteCharAt(sb.length() - 1);
                
            }
            
            sb.append("]}}");
            
        }
        else
        {
            sb.append("error,").append("\"").append(errorMessage).append("\"}}");
        }
        
        return sb.toString();
        
    }
    
    
    
}
