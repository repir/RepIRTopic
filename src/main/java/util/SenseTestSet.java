package util;

import io.github.repir.tools.Lib.HDTools;
import io.github.repir.Repository.Repository;
import io.github.repir.Retriever.Query;
import io.github.repir.RetrieverMR.RetrieverMR;
import io.github.repir.RetrieverMR.RetrieverMRInputFormat;
import io.github.repir.tools.Lib.Log;
import java.util.HashSet;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.util.Tool;
import static util.ContextTestSet.*;
import io.github.repir.tools.DataTypes.Configuration;

/**
 * Retrieve all topics from the TestSet, and store in an output file. arguments:
 * <configfile> <outputfileextension>
 *
 * @author jeroen
 */
public class SenseTestSet extends Configured implements Tool {

   public static Log log = new Log(SenseTestSet.class);

   public static void main(String[] args) throws Exception {
      Configuration conf = HDTools.readConfig(args, "{othersets}");
      //HDTools.setPriorityHigh(conf);
      System.exit(HDTools.run(conf, new SenseTestSet(), conf.getStrings("othersets", new String[0])));
   }

   @Override
   public int run(String[] args) throws Exception {
      Repository repository = new Repository((Configuration)getConf());
      HashSet<String> keywords = new HashSet<String>();
      addTerms( keywords, repository );
      for (String others : args ) {
         addTerms(  keywords, others );
      }
      RetrieverMR retriever = new RetrieverMR(repository);
      repository.getConfiguration().setInt("SenseTestSet.terms", keywords.size());
      for (String w : keywords) {
         Query q = new Query();
         q.stemmedquery = w;
         q.setStrategyClass(getConf().get("aoi.analyzer", "AOIAnalyzer4"));
         q.performStemming = false;
         retriever.addQueue(q);
      }
      RetrieverMRInputFormat.setSplitable(true);
      RetrieverMRInputFormat.setIndex(repository);
      retriever.retrieveQueue();
      return 0;
   }
}
