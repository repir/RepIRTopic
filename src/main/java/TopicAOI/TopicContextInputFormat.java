package TopicAOI;

import io.github.repir.Repository.Repository;
import io.github.repir.tools.Lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @author jeroen
 */
public class TopicContextInputFormat extends InputFormat<NullWritable, MapInputValue> {

   public static Log log = new Log(TopicContextInputFormat.class);
   public static ArrayList<TopicAOIInputSplit> list = new ArrayList<TopicAOIInputSplit>();

   public TopicContextInputFormat() {
   }
   
   @Override
   public RecordReader<NullWritable, MapInputValue> createRecordReader(InputSplit is, TaskAttemptContext tac) {
      return new KeyRecordReader();
   }

   /**
    * Add a Query request to the MapReduce job. Note that this is used as a
    * static method (i.e. can only construct one job at the same startTime).
    * <p/>
    * @param repository Repository to retrieve the Query request from
    * @param inputvalue The Query request to retrieve
    */
   public static void add(Repository repository, MapInputValue inputvalue) {
      for (int partition = 0; partition < repository.getPartitions(); partition++) {
         TopicAOIInputSplit split = new TopicAOIInputSplit(repository, partition, inputvalue);
         list.add(split);
      }
   }

   @Override
   public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
      return new ArrayList<InputSplit>(list);
   }
   
   public static int size() {
      return list.size();
   }

   public static class KeyRecordReader extends RecordReader<NullWritable, MapInputValue> {

      private TopicAOIInputSplit is;
      private int pos = 0;

      @Override
      public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
         //log.info("initialize()");
         this.is = (TopicAOIInputSplit) is;
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
         if (pos == 0) {
            pos++;
            return true;
         }
         return false;
      }

      @Override
      public NullWritable getCurrentKey() throws IOException, InterruptedException {
         return NullWritable.get();
      }

      @Override
      public MapInputValue getCurrentValue() throws IOException, InterruptedException {
         return is.inputvalue;
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
         return (pos) / (float) (1);
      }

      @Override
      public void close() throws IOException {
      }
   }
}
