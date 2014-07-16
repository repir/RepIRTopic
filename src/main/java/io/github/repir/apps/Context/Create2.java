package io.github.repir.apps.Context;

import io.github.repir.Repository.Repository;
import io.github.repir.tools.MapReduce.Configuration;
import io.github.repir.tools.Lib.Log;

/**
 * Retrieve all topics from the TestSet, and store in an output file. arguments:
 * <configfile> <outputfileextension>
 *
 * @author jeroen
 */
public class Create2 {

   public static Log log = new Log(Create2.class);

   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration(args, "{terms}");
      Repository repository = new Repository(conf);
      ContextJob job = new ContextJob(repository);
      for (String w : conf.getStrings("terms")) {
         job.addTerm(w);
      }
      job.submit();
      job.waitForCompletion(true);
   }
}
