/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent;

import com.basho.riak.client.http.response.RiakIORuntimeException;
import java.io.IOException;


/**
 *
 * @author roach
 */
public class Ping extends RiakCommand 
{
    @Override
    public String execute() {
        
        try
        {
            rawClient.ping();
            return "ok.";
        }
        catch (IOException ex)
        {
            return "{error,\"" + ex.getMessage() + "\"}.";
        }
        catch (RiakIORuntimeException ex)
        {
            return "{error,\"" + ex.getMessage() + "\"}.";
        }
        
        
        
    }
    
}
