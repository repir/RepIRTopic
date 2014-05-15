package TermInvertedSenseBuilder;

import io.github.repir.Repository.AutoTermDocumentFeature;
import io.github.repir.Repository.EntityStoredFeature;
import io.github.repir.Repository.Repository;
import io.github.repir.Repository.TermInvertedSense;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.MapReduce.Job;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SenseBuilderReduce extends Reducer<SenseKey, SenseValue, NullWritable, NullWritable> {

   public static Log log = new Log(SenseBuilderReduce.class);
   Repository repository;
   int partition;
   TermInvertedSense feature;
   HashMap<String, Integer> doclist = new HashMap<String, Integer>();
   ArrayList<EntityStoredFeature> documentfeatures = new ArrayList<EntityStoredFeature>();
   ArrayList<AutoTermDocumentFeature> termdocfeatures = new ArrayList<AutoTermDocumentFeature>();

   @Override
   protected void setup(Context context) throws IOException, InterruptedException {
      repository = new Repository(context.getConfiguration());
      partition = Job.getReducerId(context);
      feature = (TermInvertedSense)repository.getFeature(TermInvertedSense.class, "all");
      feature.startWrite(partition);
   }

   @Override
   public void reduce(SenseKey key, Iterable<SenseValue> values, Context context)
           throws IOException, InterruptedException {
      Job.reduceReport(context);
      for (SenseValue value : values) {
         //log.info("%d %d %s %s", key.termid, key.docid, ArrayTools.toString(value.pos),
         //        ArrayTools.toString(value.sense));
         feature.writeDoc(key.termid, key.docid, value.pos, value.sense);
      }
      context.progress();
   }

   @Override
   protected void cleanup(Context context) throws IOException, InterruptedException {
      feature.finishWrite();
   }
}
