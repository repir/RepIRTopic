package TopicAOI;

import io.github.repir.Repository.Repository;
import io.github.repir.Repository.Term;
import io.github.repir.Retriever.Query;
import io.github.repir.Retriever.Retriever;
import io.github.repir.TestSet.TestSet;
import io.github.repir.TestSet.Topic.TestSetTopic;
import io.github.repir.TestSet.Topic.TestSetTopicSession;
import io.github.repir.Repository.Configuration;
import io.github.repir.tools.DataTypes.Tuple2;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.MapReduce.Job;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class Build {

   public static Log log = new Log(Build.class);

   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration(args, "");
      Repository repository = new Repository(args);
      Retriever retriever = new Retriever(repository);
      Job job = new Job(repository, "Topic AOI Builder " + conf.get("repository.prefix"));
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
      ArrayList<Query> queries = new ArrayList<Query>();

      // map of <partition, map<collectionid, list<topicid, queryterm>>>
      HashMap<Integer, HashMap<Tuple2<Integer, String>, ArrayList<String>>> table = new HashMap<Integer, HashMap<Tuple2<Integer, String>, ArrayList<String>>>();
      for (TestSetTopic t : testset.topics.values()) {
         TestSetTopicSession ts = (TestSetTopicSession) t;
         Query q = testset.getQuery(t.id, retriever);
         ArrayList<Tuple2<Integer, String>> querytermlist = new ArrayList<Tuple2<Integer, String>>();
         for (String keyword : q.query.split("\\s+")) {
            Term term = repository.getTerm(keyword);
            if (term.exists()) {
               querytermlist.add(new Tuple2<Integer, String>(q.getID(), term.getProcessedTerm()));
            }
         }
         for (String collectionid : ts.clickeddocuments) {
            int partition = repository.getPartition(collectionid);
            HashMap<Tuple2<Integer, String>, ArrayList<String>> qtable = table.get(partition);
            if (qtable == null) {
               qtable = new HashMap<Tuple2<Integer, String>, ArrayList<String>>();
               table.put(partition, qtable);
            }
            for (Tuple2<Integer, String> topicterm : querytermlist) {
               ArrayList<String> docs = qtable.get(topicterm);
               if (docs == null) {
                  docs = new ArrayList<String>();
                  qtable.put(topicterm, docs);
               }
               docs.add(collectionid);
            }
         }
      }

      for (int partition : table.keySet()) {
         MapInputValue m = new MapInputValue();
         m.partition = partition;
         m.map_topicterm_documents = table.get(partition);
         TopicContextInputFormat.add(repository, m);
      }

      conf.setInt("TopicAOI.keys", TopicContextInputFormat.size() / repository.getPartitions());

      job.waitForCompletion(true);
      log.info("BuildRepository completed");
   }
}
