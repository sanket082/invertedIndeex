
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.zip.DeflaterOutputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.maven.shared.utils.StringUtils;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author owner
 */
public class Web 
{
    static FileReader fileReader;
    static Hashtable<Integer, Long[]> lexicon;
    static HashMap<String,String[]> PageURLTable;
    static HashMap<String,Integer> termIDS;
    static DB database ;
    public static final String ANSI_RESET = "\u001B[0m";
public static final String ANSI_RED = "\u001B[31m";
    static HashMap<String,Long[]> frequencyGraph;
    static DBCollection collection;
    static BasicDBObject document;
    static  MongoClient mongoClient;
    static float k1 = (float) 1.2; //constants
   static float b = (float) 0.75; //#constants
   
   
   //Calculates BM25 for a given term in that particular document . Uses the formula stated in the 
   //documentation and is used for ranking the queries
    static float BM25(int frequency,int page_size){
        float K = k1 * ((1-b) + b * ((page_size)/(200)) );
        float first_factor = 1;
        float second_factor = ((k1 + 1) * frequency) / (K + frequency);
        return first_factor * second_factor;
    }
    
    
    /*First pass to determine barrel size for each posting collection. Thsi allows us to create
    a custom size where detail for a particular term will go. While creating the frequencyGraph hashmap
    which contains how many documents the term is in and also how many times the term appears 
    altogeter in the collection. It also saves DocID, URL and data to a MongoDB database 
    for providing snippets.
    */
    static void barrelSize() throws FileNotFoundException, IOException{
    String[] tempval1;
    String keyword;
    int keyValue;
    HashMap<String,Integer>  forPage =new HashMap<String,Integer>();
        Long[] temp23 = new Long[2];
        Long[] temp45 = new Long[2];
        
    HashMap<String,Integer> mapforpage1 = new HashMap<String,Integer>();
    FileReader fileReader1 = new FileReader("E:\\msmarco-docs.tsv");
      BufferedReader br1 = new BufferedReader(fileReader1); 
      String line;
      int i=0;
      while((line=br1.readLine())!=null){
          forPage =new HashMap<String,Integer>();
          tempval1  = line.split("\t");
            if(tempval1.length!=4){
           continue;
           }
          String data = tempval1[3];   
          document = new BasicDBObject();
          BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("DocID", tempval1[0]);
            DBCursor cursor = collection.find(searchQuery);
            if(!cursor.hasNext()){
           document.put("DocID",tempval1[0]);
           document.put("url", tempval1[1]);
           document.put("data", data);
           collection.insert(document);
            }
          String[] datasplit = data.split("[ .,\"]");
           for(String a:datasplit){
               if(a.length()>40){
                   continue;
               }
               if((a.length()==1) && (a.matches("[0-9]"))){
               continue;
           }
               if(a.matches("[A-Za-z0-9]+")){
                  if(!forPage.containsKey(a)){
                      forPage.put(a, 1);
                  }
                  if(!frequencyGraph.containsKey(a)){
                      temp23 = new Long[2];
                      temp23[0]=0L;
                      temp23[1]=1L;
                      frequencyGraph.put(a, temp23);
                  }
                  else{
                      temp23 = new Long[2];
                      temp45 = frequencyGraph.get(a);
                      temp23[0]=temp45[0];
                      temp23[1]=temp45[1]+1;
                      frequencyGraph.replace(a, temp23);
                  }
           }
     }
      for (Map.Entry<String, Integer> e1 : forPage.entrySet()) {
             temp45 =frequencyGraph.get(e1.getKey());
             temp23 = new Long[2];
             temp23[0]=temp45[0]+1;
             temp23[1]=temp45[1];
             frequencyGraph.replace(e1.getKey(), temp23);
         }
      
     i++; 
}
      br1.close();
      fileReader1.close();
}
    
    
    /*sort using comparator, We are sorting all the appearences of a particular term
    in different document by using comparator. This is provided with a hashmap of a term and 
    the value contains frequency as first term and BM25 as second term. I have connverted BM25 in 
    long by multiplying it by 1000. This gives us a BM25 that is accurate up to 3 digits.
    By sorting the BM25 and then writing it to inverted inded we can extract the top 
    10 ranked results much faster by just etting the first 10 result while extraction
    */
    public static HashMap<String, Long[]> sortByValue1(HashMap<String, Long[]> hm) 
    { 
        List<Map.Entry<String, Long[]> > list = 
               new LinkedList<Map.Entry<String, Long[]> >(hm.entrySet()); 
        Collections.sort(list, new Comparator<Map.Entry<String, Long[]> >() { 
            public int compare(Map.Entry<String, Long[]> o1,  
                               Map.Entry<String, Long[]> o2) 
            { 
                return (o2.getValue()[1]).compareTo(o1.getValue()[1]); 
            } 
        }); 
        HashMap<String, Long[]> temp = new LinkedHashMap<String, Long[]>(); 
        for (Map.Entry<String, Long[]> aa : list) { 
            temp.put(aa.getKey(), aa.getValue()); 
        } 
        return temp; 
    } 
    
    // Filler fuction to check is a string can be converted to integer
    public static boolean isProperInteger(String s) {
    try { 
        Integer.parseInt(s); 
    } catch(NumberFormatException e) { 
        return false; 
    } catch(NullPointerException e) {
        return false;
    }
  
    return true;
}
    
    //Filler fuction to check is a string can be converted to integer
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
    
    // unzips the CSV file from gz format. Extracts the file from the compressed
    // format and saves it to designated location
    public static void unZip(Path source, Path target) throws IOException {

        try (GZIPInputStream gis = new GZIPInputStream(
                                      new FileInputStream(source.toFile()));
             FileOutputStream fos = new FileOutputStream(target.toFile())) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gis.read(buffer)) > 0) {
                fos.write(buffer, 0, len);
            }

        }
    }
   
    
    
    
    
    
    public static void main(String args[]) throws FileNotFoundException, IOException {
        File file = new File("E:\\poweer\\inverted");
         if (file.exists()) {
      file.delete();
                }
        

// Extracting the source file in tsv format 
        String[] tempVal;
        Path source = Paths.get("E:\\msmarco-docs.tsv.gz");
        Path target = Paths.get("E:\\poweer\\amd.tsv");
        newJavaFile.unZip(source, target);
           fileReader = new FileReader("E:\\poweer\\amd.tsv");
        frequencyGraph = new HashMap<String, Long[]>();
         mongoClient = new MongoClient("localhost", 27017);
       database = mongoClient.getDB("URL");
       collection = database.getCollection("URLCollect");
       BasicDBObject document;
        barrelSize();
        Scanner sc= new Scanner(System.in);
     FileOutputStream xtreme = new FileOutputStream("E:\\poweer\\inverted.txt");
    DataOutputStream xtreme1 =new DataOutputStream(xtreme);
    xtreme1.close();
    xtreme.close();
        RandomAccessFile raf =new RandomAccessFile("E:\\poweer\\inverted.txt","rw"); 
        lexicon= new Hashtable<Integer, Long[]>();
        HashMap<String,Long[]> foreachDocument = new HashMap<String,Long[]>();
     termIDS = new HashMap<String,Integer>();
//        fileReader = new FileReader("E:\\msmarco-docs.tsv");
        BufferedReader br1 = new BufferedReader(fileReader);
        int i=0;
        HashMap<String,ArrayList> wordposition = new HashMap<String,ArrayList>();
        String line;
        Long currentPosition=0L;
         ArrayList just;
        int temtemp=0;
        String DocID="";        
        int position=0;
        int terms1ID =0;
        int page_s = 0;
        
        
        //creates inverted index for the data
        while((line=br1.readLine())!=null){
            position=-1;
            wordposition = new HashMap<String,ArrayList>();
             HashMap<String,Integer> mapforpage = new HashMap<String,Integer>();
            tempVal  = line.split("\t");
            if(tempVal.length!=4){
           continue;
           }
             DocID = tempVal[0];
           String URL  = tempVal[1];
           String title  =tempVal[2];
           String data = tempVal[3];
           String[] lexiconTemp;
           
           String[] datasplit = data.split("[ .,\"]");
           page_s  = datasplit.length;
            for(String a:datasplit){
                position++;
                 if(a.length()>40){
                   continue;
                }
                 if((a.length()==1) && (a.matches("[0-9]"))){
               continue;
           }
                 if(a.matches("[A-Za-z0-9]+")){
                      if(termIDS.get(a)==null){
                   termIDS.put(a, terms1ID);
                   terms1ID++;
               }
                      if(!mapforpage.containsKey(a)){
                   mapforpage.put(a, 1);
               }
               else{
                   int tre =mapforpage.get(a)+1;
                   mapforpage.replace(a, tre+1);
               }
                      temtemp= termIDS.get(a);
                      if(!wordposition.containsKey(a)){
                   just = new ArrayList();
                   just.add(new Integer(position));
                   wordposition.put(a, just);
               }
                      else{
                   just= wordposition.get(a);
                   just.add(new Integer(position));
                   wordposition.replace(a, just);
               }
                      if(!lexicon.containsKey(temtemp)){
                          Long[] independent =frequencyGraph.get(a);
                           if(independent==null)
                           {
                                continue;
                            }
                           long docf = independent[0];
                           Long[]  y = new Long[]{docf,currentPosition};
                           lexicon.put(temtemp, y);
                           Long tmpx = frequencyGraph.get(a)[0]*50;
                           tmpx += frequencyGraph.get(a)[1]*50;
                           currentPosition += tmpx;
                      }
                 }
                 }
              for (Map.Entry<String, Integer> e : mapforpage.entrySet()) {
                   foreachDocument = new HashMap<String,Long[]>();
                  int temId = termIDS.get(e.getKey());
                  Long[] tepo = lexicon.get(temId);
                  just = wordposition.get(e.getKey());
                  float bm25_temp = BM25(e.getValue(), page_s);
                  if(tepo==null){
              continue;
              }
                  raf =new RandomAccessFile("E:\\poweer\\inverted","rw"); 
                  long location = tepo[1];
                  raf.seek(location);
                   int traverse= (int) ((frequencyGraph.get(e.getKey())[0])*45+ (frequencyGraph.get(e.getKey())[0])*45);
                   byte[] b=new byte[traverse];
                   raf.read(b);
                   String s = new String(b);
                   String[] docs = s.split(";");
                   if(docs.length<=1){
                       String ness = DocID.substring(1)+","+just.size()+","+bm25_temp+":";//p
                       for(int j=0;j<just.size()-1;j++){
                           ness+=just.get(j)+",";
                       }
                       ness+=just.get(just.size()-1);
                       ness+=";x\n";
                       byte[] bytes = ness.getBytes("UTF-8");
                       raf.seek(location);
                       raf.write(bytes);
//                       System.out.println(e.getKey()+"=>   "+ness);
                   }
                   else{
                       
                  String example="";
                  String heap= docs[0];
                  String[] differentDocs = heap.split(">");
                  for(String docsAndFrequency:differentDocs){
                      String[] mySplit = docsAndFrequency.split(":");
//                      System.out.println(docs);
                        if(mySplit.length<2){continue;}
                      String[] tempSplit = mySplit[0].split(",");
                      if(tempSplit.length<3){continue;}
                      String getDocId =tempSplit[0];
                      Long getFrequency = Long.parseLong(tempSplit[1]);
//                      Long bm25 = (long)(Float.parseFloat(tempSplit[2])*1000);
//                      System.out.println(bm25);
                      String[] tempSplit2 = mySplit[1].split(",");
                      Long[] tempArray = new Long[tempSplit2.length+2];
                      tempArray[0]=getFrequency;
                      tempArray[1]=(long)(Float.parseFloat(tempSplit[2])*1000);
                      for (int d=0;d<tempSplit2.length;d++){
                         if(!isProperInteger(tempSplit2[d])){continue;}
                          tempArray[d+2]=Long.parseLong(tempSplit2[d]);
                      }
                     
                      foreachDocument.put(getDocId, tempArray);
                      
                  }
                   String qw = e.getKey();
                   Integer fq =e.getValue();
                   ArrayList xml  = wordposition.get(e.getKey());
//                       System.out.println(xml.get(0).getClass());
                   Long[] qwerty = new Long[xml.size()+2];
                   qwerty[0]=Long.valueOf(xml.size());
                   qwerty[1]= (long)bm25_temp*1000;
                   for(int t123=0;t123<xml.size();t123++){
                       int pqw =  (Integer) xml.get(t123);
                       qwerty[t123+2]=Long.valueOf(pqw);
                   }
                   foreachDocument.put(DocID.substring(1), qwerty);
                   foreachDocument=sortByValue1(foreachDocument);
                      for (Entry<String, Long[]> e2 : foreachDocument.entrySet()) {
                          Long[] hers= e2.getValue();
//                          System.out.println(e2.getKey());
                          example +=e2.getKey()+",";
                          example+=hers[0]+","+hers[1]/1000+":";
                          for(int r=1;r<hers.length;r++){
                              example+=hers[r]+",";
                          }
                          example=(example.substring(0, example.length() - 1));
                          example+=">";
                      }
                      example=(example.substring(0, example.length() - 1));
                      example+=";x";
                      byte[] bytes = example.getBytes("UTF-8");
                      raf.seek(location);
                      raf.write(bytes);
//                       System.out.println(example);
                                        
                  
                   }
                 
              }

            i++;
        }
        raf.close();
        File fileOne=new File("E:\\poweer\\termID");
        FileOutputStream fos=new FileOutputStream(fileOne);
        ObjectOutputStream oos=new ObjectOutputStream(fos);
        oos.writeObject(termIDS);
        oos.flush();
        oos.close();
        fos.close();
         File fileOne1=new File("E:\\poweer\\lexicon");
        FileOutputStream fos1=new FileOutputStream(fileOne1);
        ObjectOutputStream oos1=new ObjectOutputStream(fos1);
        oos1.writeObject(lexicon);
        oos1.flush();
        oos1.close();
        fos1.close();
        File fileOne2=new File("E:\\poweer\\frequency");
        FileOutputStream fos2=new FileOutputStream(fileOne2);
        ObjectOutputStream oos2=new ObjectOutputStream(fos2);
        oos2.writeObject(frequencyGraph);
        oos2.flush();
        oos2.close();
        fos2.close();

        //zips inverted index to save space
     String sourceFile = "E:\\poweer\\inverted";
        FileOutputStream fosx = new FileOutputStream("E:\\poweer\\compressed_index.zip");
        ZipOutputStream zipOut = new ZipOutputStream(fosx);
        File fileToZip = new File(sourceFile);
        FileInputStream fisx = new FileInputStream(fileToZip);
        ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
        zipOut.putNextEntry(zipEntry);
        byte[] bytes = new byte[1024];
        int length;
        while((length = fisx.read(bytes)) >= 0) {
            zipOut.write(bytes, 0, length);
        }
        zipOut.close();
        fisx.close();
        fosx.close();
       raf.close();
    }
      
    }

