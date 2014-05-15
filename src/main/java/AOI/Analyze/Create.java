package AOI.Analyze;

import io.github.repir.Repository.AOI;
import io.github.repir.Repository.Repository;
import io.github.repir.Repository.Term;
import static io.github.repir.apps.Context.Create.getKeywords;
import io.github.repir.tools.Lib.Log;
import java.util.HashSet;

/**
 * Retrieve all topics from the TestSet, and store in an output file. arguments:
 * <configfile> <outputfileextension>
 *
 * @author jeroen
 */
public class Create {

   public static Log log = new Log(Create.class);

   public static void main(String[] args) throws Exception {
      Repository repository = new Repository(args, "collection");
      HashSet<String> keywords = getKeywords(repository, repository.configuredString("collection"));
      AnalyzeJob job = new AnalyzeJob(repository);
      for (String w : keywords) {
         Term term = repository.getTerm(w);
         AOI aoi = (AOI) repository.getFeature(AOI.class, term.getProcessedTerm());
         if (!aoi.getFile().exists() && !term.isStopword()) {
            job.addTerm(w);
            log.info("%s", w);
         }
      }
      job.submit();
      job.waitForCompletion(true);
   }
}
