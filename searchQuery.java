

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Scanner;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.maven.shared.utils.StringUtils;

/**
 *
 * @author owner
 */
public class searchQuery {
     private static final int BUFFER_SIZE = 4096;
   
     
     /*unzips the compressed file to designated path.
     Zip is a lossless compression and preseres the location information when uncompressed
     */
    public static void unzip(String zipFilePath, String destDirectory) throws IOException {
        File destDir = new File(destDirectory);
        if (!destDir.exists()) {
            destDir.mkdir();
        }
        ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFilePath));
        ZipEntry entry = zipIn.getNextEntry();
        // iterates over entries in the zip file
        while (entry != null) {
            String filePath = destDirectory + File.separator + entry.getName();
            if (!entry.isDirectory()) {
                // if the entry is a file, extracts it
                extractFile(zipIn, filePath);
            } else {
                // if the entry is a directory, make the directory
                File dir = new File(filePath);
                dir.mkdirs();
            }
            zipIn.closeEntry();
            entry = zipIn.getNextEntry();
        }
        zipIn.close();
    }

    private static void extractFile(ZipInputStream zipIn, String filePath) throws IOException {
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath));
        byte[] bytesIn = new byte[BUFFER_SIZE];
        int read = 0;
        while ((read = zipIn.read(bytesIn)) != -1) {
            bos.write(bytesIn, 0, read);
        }
        bos.close();
    }
    
    //global variable declaration
    static FileReader fileReader;  
    static DB database ;
    public static final String ANSI_RESET = "\u001B[0m";
public static final String ANSI_RED = "\u001B[31m";
    static   File toRead;
    static File toRead1;
    static ObjectInputStream ois1;
    static  FileInputStream fis1;
    static ObjectInputStream ois;
    static FileInputStream fis2;
    static RandomAccessFile raf;
    static  long startTime;
    static HashMap<String,Integer> termIDS;
    static  FileInputStream fis;
    static File toRead2;
    static  String str ;
    static int ttid,traverse;
    static Long[] lem;
    static byte[] b1;
    static long df,addr;
    static  String[] answers;
    static String permanent;
    static DBCollection collection;
    static ObjectInputStream ois2;
    static HashMap<String,Long[]> frequencyGraph;    
    static BasicDBObject document;
    static Hashtable<Integer, Long[]> lexicon;
    static  MongoClient mongoClient;
 public static boolean isProperLong(String s) {
    try { 
        Long.parseLong(s); 
    } catch(NumberFormatException e) { 
        return false; 
    } catch(NullPointerException e) {
        return false;
    }
  
    return true;
}
 
 
 
 /*
 Opens the list to search for particular term .It loads the lexicon, frequncyGraph, and termIDs from
 the file into hashmap. This does not load the entire inverted index.
 */
 public static void openList(String term) throws IOException, ClassNotFoundException{
     str  =term;
   
        
        if(!termIDS.containsKey(str)){
    System.out.println("Not found");
}
else{
ttid = termIDS.get(str);
lem =lexicon.get(ttid);
df = lem[0];
addr =lem[1];
raf = new RandomAccessFile("E:\\poweer\\inverted","rw");
raf.seek(addr);
traverse= (int) ((frequencyGraph.get(str)[0])*45+(frequencyGraph.get(str)[1])*45);

b1=new byte[traverse];

 raf.read(b1);
 
 permanent = new String(b1, StandardCharsets.US_ASCII);
 permanent = permanent.split(";")[0];
 answers = permanent.split(">");
int flag=10;
if(answers.length<10){
flag=answers.length;
}
}
 }
 
 
 /*
 This provided the top 10 ranked results for the search query. It reads the postings for the
 top 10 results and display snippets of that data to the user. It also loads the respective 
 document from the MongoDB database. This provides us with URL and snippets of the query
 as well as the number of time that query appeared in the document. it also surrounds searched query with 
 <****** *******> on both sides.
 */
public static void query(){
String doc="";
Long frq = 0L;
HashMap<String,Long[]> temp1 = new HashMap<String,Long[]>();
if(answers!=null){


 for(int j=0;j<answers.length;j++){
     frq = getFreq(answers[j]);
     String[] wert = answers[j].split(":");
     doc = wert[0].split(",")[0];
     Long freq = Long.parseLong(wert[0].split(",")[1]);
     String[] positions = wert[1].split(",");
     Long[] poss = new Long[positions.length];
     for(int ty=0;ty<positions.length;ty++){
         if(!isProperLong(positions[ty])){continue;}
         poss[ty] = Long.parseLong(positions[ty]);
     }
     temp1.put(doc, poss);
 }
   for (Map.Entry<String, Long[]> e1 : temp1.entrySet()) {
       BasicDBObject searchQuery = new BasicDBObject();
       String query = "D"+e1.getKey();
searchQuery.put("DocID", query);
DBCursor cursor = collection.find(searchQuery);
 Long[] xert = e1.getValue();
 
while (cursor.hasNext()) {
           DBObject curs = cursor.next();
    String turk =curs.get("data").toString();
    String URL= curs.get("url").toString();
    if(StringUtils.countMatches(turk, str)<1){continue;};
    String[] poped= turk.split("[ .,\"]");
    for(int der=0;der<xert.length;der++){
        long v  =xert[der];
        int web =(int) v;
        poped[web]= "<-----**** "+poped[web]+" ****---->";
    }
    ArrayList<String> snippet = new ArrayList<String>();
    for(int der=0;der<xert.length;der++){
        long v  =xert[der];
        int web =(int) v;
        int starting=0,ending=poped.length-1;
        if(web<30){
        starting =0;
        }else{
        starting =web-30;
        }
        if(ending-web>30){
            ending =  web+30;
        }
        for(int fg=starting;fg<ending;fg++){
            snippet.add(poped[fg]);
        }
    }
    System.out.println();
    turk="";
    int f=1;
    turk += URL+"\n";
    for(String ht:snippet){
    if(f%120==0){
        turk+="\n";
    }
    turk+=ht+" ";
    f++;
    }
    
//    String pcap = ANSI_RED+" *"+str+"* "+ANSI_RESET;
//    turk=turk.replaceAll(" "+str+" ",pcap);
     System.out.println(turk+"   freq =   "+frq.toString()+"\n\n");
   
    //System.out.println(fre);
}
   }
   }

}




//Gives frequncy for the current posting
 public static Long getFreq(String posting){
     String[] x = posting.split(":");
     String y= x[0].split(",")[1];
     Long z  = Long.parseLong(y);
     return z;
 }
 
 
 //Closes the list and removes all the data loaded in the main memory
 public static void closeList(RandomAccessFile r) throws IOException{
     r.close();
     ois.close();
     ois1.close();
     ois2.close();
     mongoClient.close();
 }
 
 
    public static void main(String args[]) throws FileNotFoundException, IOException, ClassNotFoundException {
        unzip("E:\\poweer\\compressed_index.zip","E:\\poweer"); 
        Scanner sc= new Scanner(System.in);
          Scanner sc1= new Scanner(System.in);
        // TODO code application logic here
           mongoClient = new MongoClient("localhost", 27017);
       database = mongoClient.getDB("URL");
       collection = database.getCollection("URLCollect");
        toRead=new File("E:\\poweer\\termID");
        fis=new FileInputStream(toRead);
        ois=new ObjectInputStream(fis);

        termIDS=(HashMap<String,Integer>)ois.readObject();
         toRead1=new File("E:\\poweer\\lexicon");
        fis1=new FileInputStream(toRead1);
        ois1=new ObjectInputStream(fis1);

        lexicon=(Hashtable<Integer, Long[]>)ois1.readObject();
        
        toRead2=new File("E:\\poweer\\frequency");
        fis2=new FileInputStream(toRead2);
        ois2=new ObjectInputStream(fis2);

        frequencyGraph=(HashMap<String,Long[]>)ois2.readObject();
       while(true){
           startTime = System.currentTimeMillis();
    System.out.println();
    System.out.println("press\n 1: To seach a query \n 2: close the search");
    int yu= sc1.nextInt();
    int flare =0;
    if(yu==1){
System.out.println("Enter a string:-------------------------------------------------------------------------------------- ");
String str= sc.nextLine(); //reads string
openList(str);
query();
flare =1;
    }
    else if(yu==2){
        if(flare==0){break;}
    closeList(raf);
    break;
}
    else{
        System.out.println("try again");
        continue;
    }
long endTime = System.currentTimeMillis();
 long times = (endTime-startTime)/1000;
    System.out.println("Time for query execution:  "+times+"ms\n\n\n");
       }
        }
    
    }

