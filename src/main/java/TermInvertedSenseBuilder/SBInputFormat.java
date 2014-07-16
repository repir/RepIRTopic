package TermInvertedSenseBuilder;

import io.github.repir.Repository.Repository;
import io.github.repir.Repository.Term;
import io.github.repir.tools.Lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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
public class SBInputFormat extends InputFormat<NullWritable, Text> {

   public static Log log = new Log(SBInputFormat.class);
   public static Repository repository;
   static ArrayList<SBInputSplit> list = new ArrayList<SBInputSplit>();

   public SBInputFormat() {
   }

   public SBInputFormat(Job job, ArrayList<Term> topics) {
      job.setInputFormatClass(this.getClass());
      job.setOutputFormatClass(NullOutputFormat.class);
      for (int partition = 0; partition < repository.getPartitions(); partition++) {
         SBInputSplit split = new SBInputSplit(repository, partition);
         for (Term t : topics) {
            Text rec = new Text( t.getProcessedTerm() );
            split.add(rec);
         }
         list.add(split);
      }
   }
   
   @Override
   public RecordReader<NullWritable, Text> createRecordReader(InputSplit is, TaskAttemptContext tac) {
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
      SBInputFormat.repository = repository;
      list = new ArrayList<SBInputSplit>();
   }

   @Override
   public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
      return new ArrayList<InputSplit>(list);
   }

   public static class KeyRecordReader extends RecordReader<NullWritable, Text> {

      private Repository repository;
      private Text current;
      private SBInputSplit is;
      private int pos = 0;

      @Override
      public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
         //log.info("initialize()");
         this.is = (SBInputSplit) is;
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
         if (pos < is.getLength()) {
            current = is.inputvalue.get(pos);
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
      public Text getCurrentValue() throws IOException, InterruptedException {
         return current;
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
         return (pos) / (float) (is.getLength());
      }

      @Override
      public void close() throws IOException {
      }
   }
}
