package util;

import java.util.ArrayList;
import io.github.repir.Repository.Repository;
import io.github.repir.Retriever.Retriever;
import io.github.repir.Repository.AOI;
import io.github.repir.Repository.AOI.Rule;
import io.github.repir.tools.Lib.ArgsParser;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Stemmer.englishStemmer;

public class listSense {

   public static Log log = new Log(listSense.class);

   public static void main(String[] args) {
      ArgsParser parsedargs = new ArgsParser(args, "configfile term");
      Repository repository = new Repository(parsedargs.get("configfile"));
      Retriever retriever = new Retriever(repository);
      AOI termsense = (AOI) repository.getFeature("AOI");
      englishStemmer stemmer = englishStemmer.get();
      int termid = repository.termToID( stemmer.stem(parsedargs.get("term")) );

      termsense.openRead();
      ArrayList<Rule> rules = termsense.read(termid);
      for ( Rule r : rules ) {
            log.printf("%s\n", r.toString(repository));
      }
   }


}
