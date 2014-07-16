package io.github.repir.apps.Context;

import io.github.repir.Repository.Repository;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.hadoop.Job;
import io.github.repir.tools.MapReduce.StringInputFormat;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * Extension of Hadoop Job, used by JobManager to start multi-threaded 
 * rerieval.
 * @author jer
 */
public class ContextJob extends Job {

   public static Log log = new Log(ContextJob.class);
   public StringInputFormat inputformat;

   public ContextJob(Repository repository) throws IOException {
      super(repository.getConfiguration(), "Context collector " + repository.configuredString("rr.conf"));
      inputformat = new StringInputFormat(repository);
      setMapOutputKeyClass(NullWritable.class);
      setMapOutputValueClass(NullWritable.class);
      setOutputKeyClass(NullWritable.class);
      setOutputValueClass(NullWritable.class);
      setMapperClass(ContextMap.class);
      setInputFormatClass(inputformat.getClass());
      setOutputFormatClass(NullOutputFormat.class);
      this.setNumReduceTasks(0);
   }

   public void addTerm(String term) {
      inputformat.add(term);
   }
}
