package com.basho.congruent;

import com.basho.congruent.operations.OperationFactory;
import com.basho.congruent.operations.RiakOperation;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.http.HTTPClientAdapter;
import gnu.getopt.Getopt;
import java.io.*;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws RiakException
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
        
        String httpBaseUrl = riakOpts[0];
        int httpPort = Integer.parseInt(riakOpts[1]);
        int pbPort = Integer.parseInt(riakOpts[2]);
        
        
        
        
        
        // Parse command input file (JSON) 
        // - base64 bucket/key/value
        // return list of command objects?
        // http://commons.apache.org/codec/api-release/overview-summary.html

        
        outFilename = (outFilename == null) ?  filename + ".out" : outFilename;
        
        
        FileWriter outputFileWriter = null;
        
        OperationFactory operationFactory = new OperationFactory(httpBaseUrl, httpPort, pbPort);
        
        try 
        {
            outputFileWriter = new FileWriter(new File(outFilename));
            BufferedWriter bufferedWriter = new BufferedWriter(outputFileWriter);
            
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readValue(new File(filename), JsonNode.class);
            
            for (JsonNode currentNode : rootNode)
            {
                String commandName = currentNode.path("command").getTextValue();
                System.out.println(commandName);
                // get command from factory
                RiakOperation currentOperation =
                        operationFactory.createOperation(currentNode);
                String result = currentOperation.execute();
                bufferedWriter.write(result);
                bufferedWriter.newLine();
                
            }
            bufferedWriter.close();
        } 
        catch (FileNotFoundException ex) 
        {
             System.err.println(ex.getMessage());
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
