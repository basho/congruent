package com.basho.congruent;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.http.HTTPClientAdapter;
import gnu.getopt.Getopt;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
       
        Getopt g = new Getopt("RiakClientTest", args, "f:h:o:");
        
        
        // Use Getopt to parse commands - Riak Node, client type, 
        // command input file.
        
        
        int c;
        String filename = null;
        String outFilename = null;
        String[] riakOpts = null;
        while ((c = g.getopt()) != -1)
        {
            
            switch(c)
            {
                case 'f':
                    filename = g.getOptarg();
                    break;
                case 'h':
                    riakOpts = g.getOptarg().split(":");
                    if (riakOpts.length < 3)
                    {
                        System.out.println("-h requires IP:port:port");
                        System.exit(-1);
                    }
                    break;
                case 'o':
                    outFilename = g.getOptarg();
                    break;
                case '?':
                    System.out.println("-f, -h, and -o require arguments");
                    System.exit(-1);
                    break;
                default:
                    break;
            }
        }
        
        if (filename == null || riakOpts == null)
        {
            System.out.println("Missing filename or riak URL");
            System.exit(-1);
        }
        // Parse command input file (JSON) 
        // - base64 bucket/key/value
        // return list of command objects?
        // http://commons.apache.org/codec/api-release/overview-summary.html

        
        outFilename = (outFilename == null) ?  filename + ".out" : outFilename;
        
        // instantiate Riak client
        String connectString = "http://" + riakOpts[0] + ":"
                + riakOpts[1] + "/riak";
                
        RiakClient riakClient = new RiakClient(connectString);
        RawClient rawClient = new HTTPClientAdapter(riakClient);
        
        
        FileWriter outputFileWriter = null;
        
        CommandFactory commandFactory = new CommandFactory();
        
        try 
        {
            outputFileWriter = new FileWriter(new File(outFilename));
            BufferedWriter bufferedWriter = new BufferedWriter(outputFileWriter);
            
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readValue(new File(filename), JsonNode.class);
            
            for (JsonNode currentNode : rootNode)
            {
                String commandName = currentNode.path("command").getTextValue();
                
                // get command from factory
                RiakCommand currentCommand =
                        commandFactory.createCommand(rawClient,currentNode);
                String result = currentCommand.execute();
                bufferedWriter.write(result);
                bufferedWriter.newLine();
                
            }
            bufferedWriter.close();
        } 
        catch (FileNotFoundException ex) 
        {
             System.err.println("Input File not Found");
             System.exit(-1);
        }
        catch (IOException ex) 
        {
            System.out.println("IO exception: " + ex.getMessage());
        }
        finally 
        {
            try 
            {
                outputFileWriter.close();
            } 
            catch (IOException ex) 
            {
                System.out.println("Could not close output file: " + ex.getMessage());
            }
        }
       
        
        System.exit(0);
        
    }
}
