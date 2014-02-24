package TopicAOI;

import TopicAOI.MapInputValue.TopicKey;
import io.github.repir.Extractor.Extractor;
import io.github.repir.Repository.Repository;
import io.github.repir.tools.Lib.HDTools;
import io.github.repir.tools.Lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import io.github.repir.Repository.DocForward;
import io.github.repir.Repository.AOI;
import io.github.repir.Repository.AOI.RuleSet;
import io.github.repir.Repository.DocLiteral;
import io.github.repir.Repository.TermInvertedSense;
import io.github.repir.Retriever.Document;
import io.github.repir.tools.DataTypes.HashMap;
import io.github.repir.tools.Lib.ArrayTools;
import io.github.repir.tools.Stemmer.englishStemmer;

public class TopicAOIMap extends Mapper<NullWritable, MapInputValue, TopicTermSense, TopicTermSense> {

   public static Log log = new Log(TopicAOIMap.class);
   private Extractor extractor;
   private Repository repository;
   private FileSystem fs;
   private TopicAOIInputSplit filesplit;
   private DocLiteral doccollectionid;
   private AOI termsense;
   HashMap<Integer, RuleSet> termsenses;
   TopicTermSense outkey;

   @Override
   protected void setup(Context context) throws IOException, InterruptedException {
      repository = new Repository(context.getConfiguration());
      fs = HDTools.getFS();
      filesplit = ((TopicAOIInputSplit) context.getInputSplit());
      doccollectionid = (DocLiteral) repository.getCollectionIDFeature();
      termsense = (AOI) repository.getFeature("AOI");
   }

   @Override
   public void map(NullWritable inkey, MapInputValue value, Context context) throws IOException, InterruptedException {
      log.info("topic %d term %s termid %d", value.topic, value.stemmedterm, value.termid);
      log.info("documents %s", value.documents);
      Document doc = new Document();
      TermInvertedSense storefeature = (TermInvertedSense) repository.getFeature("TermInvertedSense:all:" + value.stemmedterm);
      storefeature.setTerm(value.stemmedterm);
      doccollectionid.setPartition(value.partition);
      doccollectionid.setBufferSize(50 * 1000 * 4096);
      doccollectionid.openRead();
      HashSet<Integer> docid = new HashSet<Integer>();
      int id = 0;
      while (doccollectionid.next()) {
         if (value.documents.contains(doccollectionid.getValue()))
            docid.add( id );
         id++;
      }
      doccollectionid.closeRead();
      
      storefeature.setPartition(value.partition);
      storefeature.setBufferSize(50 * 1000 * 4096);
      storefeature.openRead();
      HashMap<TopicTermSense, TopicTermSense> tts = new HashMap<TopicTermSense, TopicTermSense>();
      TopicTermSense t = new TopicTermSense(value.topic, value.termid);
      t.setContextDF(docid.size());
      while (storefeature.next()) {
         if (docid.contains(storefeature.docid)) {
            log.info("found %d#%d", storefeature.docid, value.partition);
            doc.docid = storefeature.docid;
            t.addSenseDoc(storefeature.getValue(doc).sense);
         }
      }
      log.info("topic=%d termid=%d cdf=%d %s", t.getTopic(), t.getTermID(), t.getContextDf(), ArrayTools.toString(t.getAOICf()));
      context.write(t, t);
      context.progress();
   }
}
