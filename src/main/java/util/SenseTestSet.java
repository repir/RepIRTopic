package util;

import io.github.repir.Repository.Repository;
import io.github.repir.Retriever.MapReduce.Retriever;
import io.github.repir.Retriever.Query;
import io.github.repir.apps.Context.Create;
import io.github.repir.Repository.Configuration;
import io.github.repir.tools.Lib.Log;
import java.util.Collection;

/**
 * Retrieve all topics from the TestSet, and store in an output file. arguments:
 * <configfile> <outputfileextension>
 *
 * @author jeroen
 */
public class SenseTestSet  {

   public static Log log = new Log(SenseTestSet.class);

   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration(args, "{othersets}");
      Repository repository = new Repository(conf);
      Collection<String> keywords = Create.getKeywords(repository, conf.getStrings("othersets"));

      Retriever retriever = new Retriever(repository);
      repository.getConfiguration().setInt("SenseTestSet.terms", keywords.size());
      for (String w : keywords) {
         Query q = new Query();
         q.query = w;
         q.setStrategyClassname(conf.get("aoi.analyzer", "AOIAna"));
         retriever.addQueue(q);
      }
      retriever.retrieveQueue();
   }
}
