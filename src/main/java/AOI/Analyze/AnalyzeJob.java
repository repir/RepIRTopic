package AOI.Analyze;

import io.github.repir.Repository.Repository;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.MapReduce.StringInputFormat;
import io.github.repir.tools.hadoop.Job;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * Analyzes term occurrences to find rules that describe an
 * Area of Interest (AOI).
 * @author jer
 */
public class AnalyzeJob extends Job {

   public static Log log = new Log(AnalyzeJob.class);
   public StringInputFormat inputformat;

   public AnalyzeJob(Repository repository) throws IOException {
      super(repository.getConfiguration(), "AOI Analyzer " + repository.configuredString("rr.conf"));
      inputformat = new StringInputFormat(repository);
      setMapOutputKeyClass(NullWritable.class);
      setMapOutputValueClass(NullWritable.class);
      setOutputKeyClass(NullWritable.class);
      setOutputValueClass(NullWritable.class);
      setMapperClass(AnalyzeMap.class);
      setInputFormatClass(inputformat.getClass());
      setOutputFormatClass(NullOutputFormat.class);
      this.setNumReduceTasks(0);
   }

   public void addTerm(String term) {
      inputformat.add(term);
   }
}
