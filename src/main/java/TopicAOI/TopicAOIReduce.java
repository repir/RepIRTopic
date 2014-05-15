package TopicAOI;

import io.github.repir.Repository.Repository;
import io.github.repir.Repository.TopicAOI;
import io.github.repir.Repository.TopicAOI.Record;
import io.github.repir.tools.Lib.ArrayTools;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.MapReduce.Job;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TopicAOIReduce extends Reducer<TopicTermSense, TopicTermSense, NullWritable, NullWritable> {

   public static Log log = new Log(TopicAOIReduce.class);
   Repository repository;
   TopicAOI topicaoi;

   @Override
   protected void setup(Context context) throws IOException, InterruptedException {
      repository = new Repository(context.getConfiguration());
      topicaoi = (TopicAOI)repository.getFeature(TopicAOI.class);
      topicaoi.openWrite();
   }

   @Override
   public void reduce(TopicTermSense key, Iterable<TopicTermSense> values, Context context)
           throws IOException, InterruptedException {
      Job.reduceReport(context);
      Record record = (Record)topicaoi.newRecord();
      record.topic = key.getTopic();
      record.term = key.getTermID();
      record.cf = 0;
      record.df = 0;
      record.senseoccurrence = new int[65];
      record.sensedoccurrence = new int[65];
      record.sensedf = new int[65];
      for (TopicTermSense value : values) {
         int aoifreq[] = value.getAOICf();
         for (int i = 0; i < 65; i++)
            record.senseoccurrence[i] += aoifreq[i];
         int aoidfreq[] = value.getAOIDf();
         for (int i = 0; i < 65; i++)
            record.sensedoccurrence[i] += aoidfreq[i];
         int df[] = value.getDf();
         for (int i = 0; i < 65; i++)
            record.sensedf[i] += df[i];
         record.cf += value.getContextFrequency();
         record.df += value.getContextDf();
      }
      log.info("%d %d %d\n%s\n%s", record.topic, record.term, record.df, 
              ArrayTools.concat(record.senseoccurrence),
              ArrayTools.concat(record.sensedoccurrence)
              );
      topicaoi.write(record);
      context.progress();
   }

   @Override
   protected void cleanup(Context context) throws IOException, InterruptedException {
      topicaoi.closeWrite();
   }
}
