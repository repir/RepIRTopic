package util;

import io.github.repir.Repository.Repository;
import io.github.repir.Retriever.Query;
import io.github.repir.Retriever.MapReduce.Retriever;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.MapReduce.Configuration;

/**
 * Retrieve all topics from the TestSet, and store in an output file. arguments:
 * <configfile> <outputfileextension>
 *
 * @author jeroen
 */
public class SenseTestSet2  {

   public static Log log = new Log(SenseTestSet2.class);

   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration(args, "term");
      Repository repository = new Repository(conf);

      Retriever retriever = new Retriever(repository);
         Query q = new Query(repository, 0, conf.get("term"));
         q.setStrategyClassname(conf.get("aoi.analyzer", "AOIAna"));
         retriever.addQueue(q);
      retriever.retrieveQueue();
   }
}
