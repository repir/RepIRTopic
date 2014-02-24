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
import io.github.repir.Retriever.Query;
import io.github.repir.Retriever.Retriever;
import io.github.repir.TestSet.TestSet;
import io.github.repir.tools.DataTypes.Configuration;


public class Build2 extends Configured implements Tool {

   public static Log log = new Log(Build2.class);

   public static void main(String[] args) throws Exception {
      Configuration conf = HDTools.readConfig(args, "topic");
      HDTools.setPriorityHigh(conf);
      Repository repository = new Repository(conf);
      System.exit( HDTools.run(conf, new Build2(), conf.getStrings("othersets", new String[0])));
   }

   @Override
   public int run(String[] args) throws Exception {
      Configuration conf = (Configuration)getConf();
      Repository repository = new Repository( conf );
      Retriever retriever = new Retriever( repository );
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
      int topic = repository.getConfigurationInt("topic", 0);
      
      SBInputFormat.repository = new Repository( conf );
      TestSet testset = new TestSet(repository);
      Query q = testset.getQuery(topic, retriever);
      
      HashSet<String> keywords = new HashSet<String>();
      addTerms( keywords, retriever, q );
      new SBInputFormat(job, keywords);

      job.waitForCompletion(true);
      log.info("BuildRepository completed");
      return 0;
   }
}
