/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.basho.congruent.operations;

import com.basho.congruent.output.ErlangTerm;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.codec.binary.Base64;

/**
 *
 * @author roach
 */
public class GetBucketProperties extends RiakOperation
{

    @Override
    public String execute()
    {
        String bucketName = 
            new String(Base64.decodeBase64(commandNode.get("bucket").getTextValue()));
        
        ErlangTerm term = new ErlangTerm(commandName, protocolName);
        
        
        try
        {
            Bucket bucket = client.fetchBucket(bucketName).execute();
            
            term.addKeyValuePairToResults("allow_multi", 
                                          bucket.getAllowSiblings());
            term.addKeyValuePairToResults("backend", 
                                          bucket.getBackend());
            term.addKeyValuePairToResults("basic_quorum", 
                                          bucket.getBasicQuorum());
            term.addKeyValuePairToResults("big_vclock", 
                                          bucket.getBigVClock());
            
            NamedErlangFunction func = bucket.getChashKeyFunction();
            term.addKeyValuePairToResults("chash_keyfun_fun", func == null ? null : func.getFun());
            term.addKeyValuePairToResults("chash_keyfun_mod", func == null ? null : func.getMod());
            
            func = bucket.getLinkWalkFunction();
            term.addKeyValuePairToResults("linkfun_fun", func == null ? null : func.getFun());
            term.addKeyValuePairToResults("linkfun_mod", func == null ? null : func.getMod());
            
            Quorum q = bucket.getDW();
            term.addKeyValuePairToResults("dw", q == null ? null : 
                                            (q.isSymbolic() ? q.getName() 
                                            : q.getIntValue())  );
            q = bucket.getPR();
            term.addKeyValuePairToResults("pr", q == null ? null : 
                                            (q.isSymbolic() ? q.getName() 
                                            : q.getIntValue())  );
            q = bucket.getPW();
            term.addKeyValuePairToResults("pw", q == null ? null :  
                                            (q.isSymbolic() ? q.getName() 
                                            : q.getIntValue())  );
            q = bucket.getR();
            term.addKeyValuePairToResults("r", q == null ? null :  
                                            (q.isSymbolic() ? q.getName() 
                                            : q.getIntValue())  );
            q = bucket.getRW();
            term.addKeyValuePairToResults("rw", q == null ? null :  
                                            (q.isSymbolic() ? q.getName() 
                                            : q.getIntValue())  );
            q = bucket.getW();
            term.addKeyValuePairToResults("w", q == null ? null :  
                                            (q.isSymbolic() ? q.getName() 
                                            : q.getIntValue())  );
            
            term.addKeyValuePairToResults("last_write_wins", 
                                          bucket.getLastWriteWins());
            term.addKeyValuePairToResults("notfound_ok", 
                                          bucket.getNotFoundOK());
            term.addKeyValuePairToResults("n_val", 
                                          bucket.getNVal());
            term.addKeyValuePairToResults("old_vclock", 
                                          bucket.getOldVClock());
            term.addKeyValuePairToResults("search",
                                          bucket.getSearch());
            term.addKeyValuePairToResults("small_vclock", 
                                          bucket.getSmallVClock());
            term.addKeyValuePairToResults("young_vclock",
                                          bucket.getYoungVClock());
            term.addKeyValuePairToResults("search_enabled",
                                          bucket.isSearchEnabled());
            
        }
        catch (RiakRetryFailedException ex)
        {
            term.failOperation(ex.getMessage());
        }
        
        return term.toString();
        
        
    }
    
}
