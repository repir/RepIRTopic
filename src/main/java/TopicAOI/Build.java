package TopicAOI;

import TopicAOI.MapInputValue.TopicKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import io.github.repir.tools.Lib.HDTools;
import io.github.repir.Repository.Repository;
import io.github.repir.tools.Lib.Log;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import io.github.repir.Retriever.Query;
import io.github.repir.Retriever.Retriever;
import io.github.repir.TestSet.TestSet;
import io.github.repir.tools.DataTypes.Configuration;

public class Build extends Configured implements Tool {
   
   public static Log log = new Log(Build.class);
   
   public static void main(String[] args) throws Exception {
      Configuration conf = HDTools.readConfig(args, "");
      Repository repository = new Repository(conf);
      System.exit(HDTools.run(conf, new Build()));
   }
   
   @Override
   public int run(String[] args) throws Exception {
      Configuration conf = (Configuration)getConf();
      Repository repository = new Repository(conf);
      Retriever retriever = new Retriever(repository);
      Job job = new Job(conf, "Topic AOI Builder " + conf.get("repository.prefix"));
      job.setJarByClass(TopicAOIMap.class);
      
      job.setNumReduceTasks(1);
      job.setGroupingComparatorClass(TopicTermSense.FirstGroupingComparator.class);
      job.setSortComparatorClass(TopicTermSense.FirstGroupingComparator.class);
      job.setMapOutputKeyClass(TopicTermSense.class);
      job.setMapOutputValueClass(TopicTermSense.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(NullWritable.class);
      job.setInputFormatClass(TopicContextInputFormat.class);
      job.setOutputFormatClass(NullOutputFormat.class);
      
      job.setMapperClass(TopicAOIMap.class);
      job.setReducerClass(TopicAOIReduce.class);
      
      TestSet testset = new TestSet(repository);
      HashMap<Integer, HashMap<String, Integer>> qrels = testset.getQrels();
      ArrayList<Query> queries = testset.getQueries(retriever);
      
      for (Query q : queries) {
         HashSet<String> context = new HashSet<String>();
         for (Query q2 : queries) {
            if (q.domain.equals(q2.domain) && q != q2) {
               for (Map.Entry<String, Integer> e : qrels.get(q2.id).entrySet()) {
                  if (e.getValue() > 0) {
                     context.add(e.getKey());
                  }
               }
            }
         }
         retriever.tokenizeQuery(q);
         for (String term : q.stemmedquery.split("\\s+")) {
            if (term.length() > 0) {
               int termid = repository.termToID(term);
               MapInputValue m = new MapInputValue();
               m.termid = termid;
               m.topic = q.id;
               m.documents = new HashSet<String>(context);
               TopicContextInputFormat.add(repository, m);
            }
         }
      }
      getConf().setInt("TopicAOI.keys", TopicContextInputFormat.size() / repository.getPartitions());
      
      job.waitForCompletion(true);
      log.info("BuildRepository completed");
      return 0;
   }
}
