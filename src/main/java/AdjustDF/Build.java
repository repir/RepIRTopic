package AdjustDF;

import io.github.repir.Repository.Repository;
import io.github.repir.tools.MapReduce.Job;
import io.github.repir.apps.Context.Create;
import io.github.repir.Repository.Configuration;
import io.github.repir.tools.Lib.Log;
import java.util.Collection;
import org.apache.hadoop.io.NullWritable;

public class Build {

   public static Log log = new Log(Build.class);

   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration(args, "{othersets}");
      conf.setPriorityHigh();
      Repository repository = new Repository(conf);
      Job job = new Job(repository, "Sense DF Adjust " + conf.get("repository.prefix"));

      int partitions = conf.getInt("repository.partitions", -1);
      job.setNumReduceTasks(partitions);
      job.setPartitionerClass(SenseKey.partitioner.class);
      job.setGroupingComparatorClass(SenseKey.FirstGroupingComparator.class);
      job.setSortComparatorClass(SenseKey.SecondarySort.class);
      job.setMapOutputKeyClass(SenseKey.class);
      job.setMapOutputValueClass(SenseValue.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(NullWritable.class);

      job.setMapperClass(AdjustDFMap.class);
      job.setReducerClass(AdjustDFReduce.class);

      AdjustDFInputFormat.repository = new Repository( conf );
      Collection<String> keywords = Create.getKeywords(repository, conf.getStrings("othersets"));
      new AdjustDFInputFormat(job, keywords);

      job.waitForCompletion(true);
      log.info("BuildRepository completed");
   }
}
