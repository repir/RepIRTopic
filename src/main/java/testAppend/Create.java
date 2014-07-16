package testAppend;

import io.github.repir.Repository.Repository;
import io.github.repir.tools.MapReduce.Configuration;
import io.github.repir.tools.Lib.Log;
import java.util.UUID;

/**
 * Retrieve all topics from the TestSet, and store in an output file. arguments:
 * <configfile> <outputfileextension>
 *
 * @author jeroen
 */
public class Create {

   public static Log log = new Log(Create.class);

   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration(args, "number");
      Repository repository = new Repository(conf);
      TestJob job = new TestJob(repository);
      for (int i = 0; i < Integer.parseInt(conf.get("number")); i++) {
         job.addTerm(UUID.randomUUID().toString());
      }
      job.submit();
      job.waitForCompletion(true);
   }
}
