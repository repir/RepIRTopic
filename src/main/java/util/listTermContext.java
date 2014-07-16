package util;

import java.util.ArrayList;
import java.util.HashMap;
import io.github.repir.Repository.Repository;
import io.github.repir.Retriever.Retriever;
import io.github.repir.Repository.TermContext;
import io.github.repir.Repository.TermContext.Doc;
import io.github.repir.Repository.TermContext.Sample;
import io.github.repir.tools.Lib.ArgsParser;
import io.github.repir.tools.Lib.ArrayTools;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Stemmer.englishStemmer;

public class listTermContext {

   public static Log log = new Log(listTermContext.class);

   public static void main(String[] args) {
      ArgsParser parsedargs = new ArgsParser(args, "configfile term");
      Repository repository = new Repository(parsedargs.get("configfile"));
      Retriever retriever = new Retriever(repository);
      englishStemmer stemmer = englishStemmer.get();
      TermContext termcontext = (TermContext) repository.getFeature(TermContext.class, stemmer.stem(parsedargs.get("term")));
      HashMap<Doc, ArrayList<Sample>> samples = termcontext.readSamples();
      for ( ArrayList<Sample> doc : samples.values() ) {
         for (Sample sample : doc ) {
            log.printf("%s %s", ArrayTools.concat(sample.leftcontext), ArrayTools.concat(sample.rightcontext));
         }
      }
   }
}
