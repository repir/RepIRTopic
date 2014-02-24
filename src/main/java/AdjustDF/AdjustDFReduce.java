package AdjustDF;

import io.github.repir.Repository.EntityStoredFeature;
import io.github.repir.Repository.Repository;
import io.github.repir.tools.Lib.HDTools;
import io.github.repir.tools.Lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import io.github.repir.Strategy.Collector.AOICollector;
import io.github.repir.Repository.AOI;
import io.github.repir.Repository.AOI.Record;
import io.github.repir.Repository.AOI.Rule;
import io.github.repir.Repository.AutoTermDocumentFeature;

public class AdjustDFReduce extends Reducer<SenseKey, SenseValue, NullWritable, NullWritable> {

   public static Log log = new Log(AdjustDFReduce.class);
   Repository repository;
   int partition;
   AOI feature;
   HashMap<String, Integer> doclist = new HashMap<String, Integer>();
   ArrayList<EntityStoredFeature> documentfeatures = new ArrayList<EntityStoredFeature>();
   ArrayList<AutoTermDocumentFeature> termdocfeatures = new ArrayList<AutoTermDocumentFeature>();
   Collection<Record> keys;

   @Override
   protected void setup(Context context) throws IOException, InterruptedException {
      repository = new Repository(context.getConfiguration());
      partition = HDTools.getReducerId(context);
      feature = (AOI)repository.getFeature("AOI");
      feature.openRead();
      keys = feature.getKeys();
   }

   @Override
   public void reduce(SenseKey key, Iterable<SenseValue> values, Context context)
           throws IOException, InterruptedException {
      HDTools.reduceReport(context);
      Record record = null;
      for (Record r : keys) {
         if (r.term == key.termid) {
            record = r;
            break;
         }
      }
      ArrayList<Rule> rules = feature.read(key.termid);
      for (Rule r : rules)
         r.df = 0;
      for (SenseValue value : values) {
         for (Rule r : rules) {
            if ((value.sense & (1l << r.sense)) != 0) {
               r.df += value.freq;
            }
         }
      }
      record.rules = AOICollector.getRules(rules);
      context.progress();
   }

   @Override
   protected void cleanup(Context context) throws IOException, InterruptedException {
      feature.openWrite();
      for (Record r :keys) {
         feature.write(r);
      }
      feature.closeWrite();
   }
}
