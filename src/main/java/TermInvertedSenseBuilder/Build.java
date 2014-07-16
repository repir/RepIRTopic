package TermInvertedSenseBuilder;

import static AOI.Analyze.Transfer.getKeywords;
import io.github.repir.Repository.Repository;
import io.github.repir.Repository.Term;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.hadoop.Job;
import java.util.ArrayList;
import org.apache.hadoop.io.NullWritable;


public class Build {

   public static Log log = new Log(Build.class);

   public static void main(String[] args) throws Exception {
      Repository repository = new Repository( args[0] );
      Job job = new Job(repository.getConfiguration(), "Sense Builder " + repository.configuredString("repository.prefix"));

      int partitions = repository.configuredInt("repository.partitions", -1);
      job.setNumReduceTasks(partitions);
      job.setPartitionerClass(SenseKey.partitioner.class);
      job.setGroupingComparatorClass(SenseKey.FirstGroupingComparator.class);
      job.setSortComparatorClass(SenseKey.SecondarySort.class);
      job.setMapOutputKeyClass(SenseKey.class);
      job.setMapOutputValueClass(SenseValue.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(NullWritable.class);

      job.setMapperClass(SenseBuilderMap.class);
      job.setReducerClass(SenseBuilderReduce.class);

      SBInputFormat.repository = repository;
      ArrayList<Term> keywords = getKeywords(repository);
      new SBInputFormat(job, keywords);

      job.waitForCompletion(true);
      log.info("BuildRepository completed");
   }
}
