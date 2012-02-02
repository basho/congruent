/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent.operations;

import com.basho.riak.client.RiakException;
import com.basho.riak.client.http.response.RiakIORuntimeException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author roach
 */
public class Ping extends RiakOperation 
{
    @Override
    public String execute() {
        
        try
        {
            riakClient.ping();
            return "ok.";
        }
        catch (RiakException ex)
        {
            return "{error,\"" + ex.getMessage() + "\"}.";
        }
        
        
        
        
    }
    
}
