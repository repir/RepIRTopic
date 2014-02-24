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
import util.ContextTestSet;
import io.github.repir.Retriever.Document;
import io.github.repir.Retriever.Query;
import io.github.repir.Retriever.Retriever;
import io.github.repir.RetrieverMR.RetrieverMR;
import io.github.repir.RetrieverMR.RetrieverMRInputFormat;
import io.github.repir.TestSet.ResultFile;
import io.github.repir.TestSet.TestSet;
import io.github.repir.tools.DataTypes.Configuration;

public class Build3 extends Configured implements Tool {

   public static Log log = new Log(Build3.class);
   public int k = 10;

   public static void main(String[] args) throws Exception {
      Configuration conf = HDTools.readConfig(args, "{othersets}");
      Repository repository = new Repository(conf);
      System.exit(HDTools.run(conf, new Build3(), conf.getStrings("othersets", new String[0])));
   }

   @Override
   public int run(String[] args) throws Exception {
      Configuration conf = (Configuration)getConf();
      Repository repository = new Repository(conf);
      Retriever retriever = new Retriever(repository);
      Job job = new Job(conf, "Topic AOI Builder " + conf.get("repository.prefix"));
      job.setJarByClass(TopicAOIMap3.class);

      job.setNumReduceTasks(1);
      job.setGroupingComparatorClass(TopicTermSense.FirstGroupingComparator.class);
      job.setSortComparatorClass(TopicTermSense.FirstGroupingComparator.class);
      job.setMapOutputKeyClass(TopicTermSense.class);
      job.setMapOutputValueClass(TopicTermSense.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(NullWritable.class);
      job.setInputFormatClass(TopicContextInputFormat.class);
      job.setOutputFormatClass(NullOutputFormat.class);

      job.setMapperClass(TopicAOIMap3.class);
      job.setReducerClass(TopicAOIReduce.class);

      TestSet testset = new TestSet(repository);

      ArrayList<Query> queries = getTopK(args);

      for (Query q : queries) {
         HashSet<String> context = new HashSet<String>();
         int count = 0;
         for (Document doc : q.queryresults) {
            context.add(doc.getLiteral("DocLiteral:collectionid"));
            if (++count >= k) {
               break;
            }
         }
         retriever.tokenizeQuery(q);
         HashSet<String> terms = new HashSet<String>();
         ContextTestSet.addTerms(terms, retriever, q);
         for (String term : terms) {
            int termid = repository.termToID(term);
            MapInputValue m = new MapInputValue();
            m.termid = termid;
            m.topic = q.id;
            m.documents = new HashSet<String>(context);
            TopicContextInputFormat.add(repository, m);
         }
      }
      getConf().setInt("TopicAOI.keys", TopicContextInputFormat.size() / repository.getPartitions());

      job.waitForCompletion(true);
      log.info("BuildRepository completed");
      return 0;
   }

   public ArrayList<Query> getTopK(String others[]) {
      Repository repository = new Repository((Configuration)getConf());
      RetrieverMR retriever = new RetrieverMR(repository);
      io.github.repir.TestSet.TestSet testset = new TestSet(repository);
      ArrayList<Query> queries = testset.getQueries(retriever);
      for (String o : others) {
         Configuration configuration2 = HDTools.readConfig(o);
         Repository repository2 = new Repository(configuration2);
         TestSet testset2 = new TestSet(repository2);
         queries.addAll(testset2.getQueries(retriever));
      }
      for (Query q : queries) {
         q.documentlimit = k;
      }
      retriever.addQueue(queries);
      RetrieverMRInputFormat.setSplitable(true);
      RetrieverMRInputFormat.setIndex(repository);
      ResultFile out = new ResultFile(repository, io.github.repir.TestSet.TestSet.getResultsFile(repository, repository.getConfigurationString("resultsfileext")));
      return retriever.retrieveQueue();
   }
}
