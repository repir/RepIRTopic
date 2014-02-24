package TermInvertedSenseBuilder;

import io.github.repir.Extractor.Extractor;
import io.github.repir.Repository.Repository;
import io.github.repir.tools.Lib.HDTools;
import io.github.repir.tools.Lib.Log;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import io.github.repir.Repository.DocForward;
import io.github.repir.Repository.TermInverted;
import io.github.repir.Retriever.Document;
import io.github.repir.Repository.AOI.RuleSet;
import io.github.repir.tools.Lib.ArrayTools;
import io.github.repir.tools.Stemmer.englishStemmer;

public class SenseBuilderMap extends Mapper<NullWritable, MapInputWritable, SenseKey, SenseValue> {

   public static Log log = new Log(SenseBuilderMap.class);
   private Extractor extractor;
   private Repository repository;
   private FileSystem fs;
   private SBInputSplit filesplit;
   SenseKey outkey;
   SenseValue outvalue = new SenseValue();

   @Override
   protected void setup(Context context) throws IOException, InterruptedException {
      repository = new Repository(context.getConfiguration());
      fs = HDTools.getFS();
      filesplit = ((SBInputSplit) context.getInputSplit());
   }

   @Override
   public void map(NullWritable inkey, MapInputWritable value, Context context) throws IOException, InterruptedException {
      int termid = repository.termToID( value.stemmedterm );
      RuleSet rules = new RuleSet(repository, termid);
      DocForward forward = (DocForward)repository.getFeature("DocForward:all");
      forward.setBufferSize(4096 * 10000);
      TermInverted postinglist = (TermInverted)repository.getFeature("TermInverted:all:" + value.stemmedterm);
      postinglist.setPartition(value.partition);
      postinglist.setTerm(value.stemmedterm);
      postinglist.setBufferSize(4096 * 10000);
      postinglist.openRead();
      Document doc = new Document();
      doc.partition = value.partition;
      while (postinglist.next()) {
         doc.docid = postinglist.docid;
         int pos[] = postinglist.getValue(doc);
         //log.info("term %s docid %d %s", value.stemmedterm, doc.docid, ArrayTools.toString(pos));
         forward.read(doc);
         int content[] = forward.getValue();
         long sense[] = rules.matchAll(content, pos);
         outvalue.pos = pos;
         outvalue.sense = sense;
         //for (long s : sense) {
         //   log.info( "%64s", Long.toBinaryString(s));
         //}
         outkey = SenseKey.createKey(value.partition, termid, doc.docid);
         context.write(outkey, outvalue);
      }
      context.progress();
   }
}
