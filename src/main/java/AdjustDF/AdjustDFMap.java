package AdjustDF;

import TermInvertedSenseBuilder.*;
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
import io.github.repir.Repository.TermInvertedSense;
import io.github.repir.Repository.TermInvertedSense.SensePos;
import io.github.repir.tools.Lib.ArrayTools;
import io.github.repir.tools.Stemmer.englishStemmer;

public class AdjustDFMap extends Mapper<NullWritable, MapInputWritable, SenseKey, SenseValue> {

   public static Log log = new Log(AdjustDFMap.class);
   private Extractor extractor;
   private Repository repository;
   private FileSystem fs;
   private AdjustDFInputSplit filesplit;
   SenseKey outkey;
   SenseValue outvalue = new SenseValue();

   @Override
   protected void setup(Context context) throws IOException, InterruptedException {
      repository = new Repository(context.getConfiguration());
      fs = HDTools.getFS();
      filesplit = ((AdjustDFInputSplit) context.getInputSplit());
   }

   @Override
   public void map(NullWritable inkey, MapInputWritable value, Context context) throws IOException, InterruptedException {
      int termid = repository.termToID( value.stemmedterm );
      RuleSet rules = new RuleSet(repository, termid);
      TermInvertedSense postinglist = (TermInvertedSense)repository.getFeature("TermInvertedSense:all:" + value.stemmedterm);
      postinglist.setPartition(value.partition);
      postinglist.setTerm( value.stemmedterm );
      postinglist.setBufferSize(4096 * 10000);
      postinglist.openRead();
      Document doc = new Document();
      doc.partition = value.partition;
      while (postinglist.next()) {
         doc.docid = postinglist.docid;
         SensePos pos = postinglist.getValue(doc);
         long senseT = 0;
         for (long s : pos.sense)
            senseT |= s;
         outvalue.freq = pos.pos.length;
         outvalue.sense = senseT;
         outkey = SenseKey.createKey(value.partition, termid);
         context.write(outkey, outvalue);
      }
      context.progress();
   }
}
