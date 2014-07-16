package TopicAOI;

import io.github.repir.Extractor.Extractor;
import io.github.repir.Repository.AOI.RuleSet;
import io.github.repir.Repository.DocLiteral;
import io.github.repir.Repository.Repository;
import io.github.repir.Repository.TermInvertedSense;
import io.github.repir.Retriever.Document;
import java.util.HashMap;
import io.github.repir.tools.DataTypes.Tuple2;
import io.github.repir.tools.Lib.ArrayTools;
import io.github.repir.tools.Lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.TreeSet;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class TopicAOIMap extends Mapper<NullWritable, MapInputValue, TopicTermSense, TopicTermSense> {

   public static Log log = new Log(TopicAOIMap.class);
   private Extractor extractor;
   private Repository repository;
   private TopicAOIInputSplit filesplit;
   private DocLiteral doccollectionid;
   HashMap<Integer, RuleSet> termsenses;
   TopicTermSense outkey;

   @Override
   protected void setup(Context context) throws IOException, InterruptedException {
      repository = new Repository(context.getConfiguration());
      filesplit = ((TopicAOIInputSplit) context.getInputSplit());
      doccollectionid = (DocLiteral) repository.getCollectionIDFeature();
   }

   @Override
   public void map(NullWritable inkey, MapInputValue value, Context context) throws IOException, InterruptedException {
      log.info("partition %d", value.partition);

      // list of documents to scan
      HashSet<String> documents = combineDocuments(value);
      HashMap<String, Integer> docmap = getDocMap(value.partition, documents);
      
      
      Document doc = new Document();
      
      for (Tuple2<Integer, String> topicterm : value.map_topicterm_documents.keySet()) {
         log.info("Term %d %s", topicterm.value1, topicterm.value2);
         TermInvertedSense storefeature = (TermInvertedSense) 
         repository.getFeature(TermInvertedSense.class, "all", topicterm.value2);
         storefeature.setTerm(repository.getProcessedTerm(topicterm.value2));
         storefeature.setPartition(value.partition);
         storefeature.setBufferSize(50 * 1000 * 4096);
         storefeature.openRead();
         HashMap<TopicTermSense, TopicTermSense> tts = new HashMap<TopicTermSense, TopicTermSense>();
         int termid = repository.termToID(topicterm.value2);
         TopicTermSense t = new TopicTermSense(topicterm.value1, termid);
         TreeSet<Integer> tt_docs = getDocIDs(docmap, value.map_topicterm_documents.get(topicterm));
         log.info("documents %s %s", value.map_topicterm_documents.get(topicterm), tt_docs);
         t.setContextDF(tt_docs.size());
         while (storefeature.next()) {
            if (tt_docs.contains(storefeature.docid)) {
               log.info("found %d#%d", storefeature.docid, value.partition);
               doc.docid = storefeature.docid;
               t.addSenseDoc(storefeature.getValue(doc).sense);
            }
         }
         storefeature.closeRead();
         log.info("topic=%d termid=%d cdf=%d %s", t.getTopic(), t.getTermID(), t.getContextDf(), ArrayTools.concat(t.getAOICf()));
         context.write(t, t);
         context.progress();
      }
   }
   
   /**
    * set of documents needed to determine correct term senses
    * @param value
    * @return 
    */
   public HashSet<String> combineDocuments(MapInputValue value) {
      HashSet<String> documents = new HashSet<String>();
      for (ArrayList<String> d : value.map_topicterm_documents.values())
         documents.addAll(d);
      return documents;
   }
   
   /**
    * @param partition
    * @param documents
    * @return Map of docid's to collectionID's
    */
   public HashMap<String, Integer> getDocMap(int partition, HashSet<String> documents) {
      DocLiteral doccollectionid = (DocLiteral) repository.getCollectionIDFeature();
      doccollectionid.setPartition(partition);
      doccollectionid.setBufferSize(50 * 1000 * 4096);
      doccollectionid.openRead();
      HashMap<String, Integer> docid = new HashMap<String, Integer>();
      int id = 0;
      while (doccollectionid.next()) {
         if (documents.contains(doccollectionid.getValue()))
            docid.put( doccollectionid.getValue(), id );
         id++;
      }
      doccollectionid.closeRead();
      return docid;
   }
   
   // convert collectionID's to docIDs
   public TreeSet<Integer> getDocIDs(HashMap<String, Integer> map, Collection<String> docs) {
      TreeSet<Integer> docids = new TreeSet<Integer>();
      for (String doc : docs) {
         docids.add(map.get(doc));
      }
      return docids;
   }
}
