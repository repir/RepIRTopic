package util;

import java.util.ArrayList;
import java.util.Collection;
import io.github.repir.Repository.Repository;
import io.github.repir.Retriever.Retriever;
import io.github.repir.Repository.AOI;
import io.github.repir.Repository.AOI.Record;
import io.github.repir.Repository.AOI.Rule;
import io.github.repir.Repository.TermString;
import io.github.repir.Strategy.AOIAnalyzer4;
import io.github.repir.tools.Lib.ArgsParser;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Stemmer.englishStemmer;

public class listRules {

   public static Log log = new Log(listRules.class);

   public static void main(String[] args) {
      ArgsParser parsedargs = new ArgsParser(args, "configfile [term]");
      Repository repository = new Repository(parsedargs.get("configfile"));
      Retriever retriever = new Retriever(repository);
      englishStemmer stemmer = englishStemmer.get();

      AOIAnalyzer4 t = new AOIAnalyzer4( retriever );
      AOI termsense = (AOI) repository.getFeature("AOI");
      termsense.openRead();
      if (parsedargs.get("term") != null) {
      int termid = repository.termToID(stemmer.stem(parsedargs.get("term")));
      log.info("termid %d", termid);
      ArrayList<Rule> rules = termsense.read(termid);
      for (Rule r : rules) {
         log.printf("%s", r.toString(repository));
      }
      } else {
         TermString termstring = (TermString)repository.getFeature("TermString");
         Collection<Record> keys = termsense.getKeys();
         for (Record key :keys) {
            log.info("term %d %s %d", key.term, termstring.readValue(key.term), key.rules.length);
         }
      }

   }


}
