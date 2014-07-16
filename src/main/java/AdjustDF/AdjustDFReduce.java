package AdjustDF;

import io.github.repir.Repository.AOI;
import io.github.repir.Repository.AOI.Record;
import io.github.repir.Repository.AOI.Rule;
import io.github.repir.Repository.AutoTermDocumentFeature;
import io.github.repir.Repository.EntityStoredFeature;
import io.github.repir.Repository.Repository;
import io.github.repir.Repository.Term;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.hadoop.Job;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AdjustDFReduce extends Reducer<SenseKey, SenseValue, NullWritable, NullWritable> {

   public static Log log = new Log(AdjustDFReduce.class);
   Repository repository;
   int partition;
   AOI aoi;
   HashMap<String, Integer> doclist = new HashMap<String, Integer>();
   ArrayList<EntityStoredFeature> documentfeatures = new ArrayList<EntityStoredFeature>();
   ArrayList<AutoTermDocumentFeature> termdocfeatures = new ArrayList<AutoTermDocumentFeature>();
   Collection<Record> keys;

   @Override
   protected void setup(Context context) throws IOException, InterruptedException {
      repository = new Repository(context.getConfiguration());
      partition = Job.getReducerId(context);
   }

   @Override
   public void reduce(SenseKey key, Iterable<SenseValue> values, Context context)
           throws IOException, InterruptedException {
      Job.reduceReport(context);
      Term term = repository.getTerm(key.termid);
      AOI aoi = (AOI)repository.getFeature(AOI.class, term.getProcessedTerm());
      keys = aoi.getKeys();
      ArrayList<Rule> rules = aoi.readRules();
      for (Rule r : rules)
         r.df = 0;
      for (SenseValue value : values) {
         for (Rule r : rules) {
            if ((value.sense & (1l << r.sense)) != 0) {
               r.df += value.freq;
            }
         }
      }
      aoi.openWrite();
      for (Rule r : rules)
         aoi.write(aoi.ceateRecord(r));
      aoi.closeWrite();
      context.progress();
   }

   @Override
   protected void cleanup(Context context) throws IOException, InterruptedException {
      aoi.openAppend();
      for (Record r :keys) {
         aoi.write(r);
      }
      aoi.closeWrite();
   }
}
