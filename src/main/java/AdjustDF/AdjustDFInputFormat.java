package AdjustDF;

import io.github.repir.Repository.Repository;
import io.github.repir.Repository.Term;
import io.github.repir.tools.Lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * A custom implementation of Hadoop's InputFormat, that holds the InputSplits
 * that are to be retrieved. This class should be used as static, using
 * {@link #setIndex(Repository.Repository)} to initialize and 
 * {@link #add(Repository.Repository, IndexReader.Query) }
 * to add Query requests to the MapReduce job. Internally, a separate InputSplit
 * is created for each repository partition. Whenever a Query request is added,
 * it is added to each Split.
 * <p/>
 * @author jeroen
 */
public class AdjustDFInputFormat extends InputFormat<NullWritable, MapInputWritable> {

   public static Log log = new Log(AdjustDFInputFormat.class);
   public static Repository repository;
   static ArrayList<AdjustDFInputSplit> list = new ArrayList<AdjustDFInputSplit>();

   public AdjustDFInputFormat() {
   }

   public AdjustDFInputFormat(Job job, Collection<String> topics) {
      job.setInputFormatClass(this.getClass());
      job.setOutputFormatClass(NullOutputFormat.class);
      for (String t : topics) {
         MapInputWritable m = new MapInputWritable();
         m.term = t;
         add( repository, m );
      }
   }
   
   @Override
   public RecordReader<NullWritable, MapInputWritable> createRecordReader(InputSplit is, TaskAttemptContext tac) {
      return new KeyRecordReader();
   }

   /**
    * Initialize the InputFormat to use the given repository and clear the inputvalue
    * of splits. Note that the Queries for one job should use the same
    * Repository.
    * <p/>
    * @param repository
    */
   public static void setIndex(Repository repository) {
      AdjustDFInputFormat.repository = repository;
      list = new ArrayList<AdjustDFInputSplit>();
   }

   /**
    * Add a Query request to the MapReduce job. Note that this is used as a
    * static method (i.e. can only construct one job at the same startTime).
    * <p/>
    * @param repository Repository to retrieve the Query request from
    * @param stemmedterm The Query request to retrieve
    */
   public static void add(Repository repository, MapInputWritable term) {
      for (int partition = 0; partition < repository.getPartitions(); partition++) {
         MapInputWritable rec = term.clone( partition );
         AdjustDFInputSplit split = new AdjustDFInputSplit(repository, partition, rec);
         list.add(split);
      }
   }

   @Override
   public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
      return new ArrayList<InputSplit>(list);
   }

   public static class KeyRecordReader extends RecordReader<NullWritable, MapInputWritable> {

      private Repository repository;
      private MapInputWritable current;
      private AdjustDFInputSplit is;
      private int pos = 0;

      @Override
      public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
         //log.info("initialize()");
         this.is = (AdjustDFInputSplit) is;
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
      public MapInputWritable getCurrentValue() throws IOException, InterruptedException {
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
