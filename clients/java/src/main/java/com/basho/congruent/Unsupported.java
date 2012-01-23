/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent;

/**
 *
 * @author roach
 */
public class Unsupported extends RiakCommand 
{

    @Override
    public String execute() 
    {
        return "unsupported.";
    }
    
}
