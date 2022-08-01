
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;

// cleans up the index file. Execute after creating inverted index and after searching a query
public class ManualCleanUp {

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        try
        { 
            Files.deleteIfExists(Paths.get("E:\\poweer\\inverted")); 
        } 
        catch(NoSuchFileException e) 
        { 
            System.out.println("No such file/directory exists"); 
        } 
        catch(DirectoryNotEmptyException e) 
        { 
            System.out.println("Directory is not empty."); 
        } 
        catch(IOException e) 
        { 
            System.out.println(e); 
        } 
          
        System.out.println("Deletion successful."); 
    
    }
}
