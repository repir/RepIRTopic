package util;

import io.github.repir.Repository.Repository;
import io.github.repir.apps.Context.Create;
import io.github.repir.tools.Lib.Log;
import java.util.Collection;

/**
 * Retrieve all topics from the TestSet, and store in an output file. arguments:
 * <configfile> <outputfileextension>
 *
 * @author jeroen
 */
public class showKeywords  {

   public static Log log = new Log(showKeywords.class);

   public static void main(String[] args) throws Exception {
      Repository repository = new Repository(args, "{othersets}");
      Collection<String> keywords = Create.getKeywords(repository, repository.configuredStrings("othersets"));
      log.info("%s", keywords);
   }
}
