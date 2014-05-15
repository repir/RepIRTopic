package util;

import io.github.repir.Repository.AOI;
import io.github.repir.Repository.AOI.Rule;
import io.github.repir.Repository.Repository;
import io.github.repir.Repository.Term;
import io.github.repir.Retriever.Retriever;
import io.github.repir.tools.Lib.ArgsParser;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Stemmer.englishStemmer;
import java.util.ArrayList;

public class listSense {

   public static Log log = new Log(listSense.class);

   public static void main(String[] args) {
      ArgsParser parsedargs = new ArgsParser(args, "configfile term");
      Repository repository = new Repository(parsedargs.get("configfile"));
      Term term = repository.getTerm(parsedargs.get("term"));
      AOI termsense = (AOI) repository.getFeature(AOI.class, term.getProcessedTerm());

      ArrayList<Rule> rules = termsense.readRules();
      for ( Rule r : rules ) {
            log.printf("%s\n", r.toString(repository));
      }
   }


}
