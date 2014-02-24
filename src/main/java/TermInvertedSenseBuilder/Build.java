package TermInvertedSenseBuilder;

import io.github.repir.tools.Lib.HDTools;
import io.github.repir.Repository.Repository;
import io.github.repir.tools.Lib.Log;
import java.util.HashSet;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import static util.ContextTestSet.*;
import io.github.repir.tools.DataTypes.Configuration;


public class Build extends Configured implements Tool {

   public static Log log = new Log(Build.class);

   public static void main(String[] args) throws Exception {
      Configuration conf = HDTools.readConfig(args, "{othersets}");
      HDTools.setPriorityHigh(conf);
      Repository repository = new Repository(conf);
      System.exit( HDTools.run(conf, new Build(), conf.getStrings("othersets", new String[0])));
   }

   @Override
   public int run(String[] args) throws Exception {
      Configuration conf = (Configuration)getConf();
      Repository repository = new Repository( conf );
      Job job = new Job(conf, "Sense Builder " + conf.get("repository.prefix"));
      job.setJarByClass(SenseBuilderMap.class);

      int partitions = conf.getInt("repository.partitions", -1);
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

      SBInputFormat.repository = new Repository( conf );
      HashSet<String> keywords = new HashSet<String>();
      addTerms( keywords, repository );
      for (String others : args ) {
         addTerms(  keywords, others );
      }
      new SBInputFormat(job, keywords);

      job.waitForCompletion(true);
      log.info("BuildRepository completed");
      return 0;
   }
}
