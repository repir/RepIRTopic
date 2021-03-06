package TermInvertedSenseBuilder;

import io.github.repir.Repository.AOI;
import io.github.repir.Repository.AOI.Rule;
import io.github.repir.Repository.Repository;
import io.github.repir.Repository.Term;
import io.github.repir.Repository.TermInverted;
import io.github.repir.Repository.TermInvertedSense;
import io.github.repir.Retriever.Document;
import io.github.repir.Retriever.Retriever;
import io.github.repir.tools.Lib.ArgsParser;
import io.github.repir.tools.Lib.ArrayTools;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Stemmer.englishStemmer;
import java.util.ArrayList;
import util.*;

public class listTerm {

   public static Log log = new Log(listSense.class);

   public static void main(String[] args) {
      englishStemmer stemmer = englishStemmer.get();
      ArgsParser parsedargs = new ArgsParser(args, "configfile partition term");
      Repository repository = new Repository(parsedargs.get("configfile"));
      int partition = parsedargs.getInt("partition");
      Term term = repository.getTerm(parsedargs.get("term"));
      Retriever retriever = new Retriever(repository);
      TermInverted termsense = (TermInverted) repository.getFeature(TermInverted.class, "all", term.getProcessedTerm() );
      termsense.setTerm( term );
      termsense.setPartition(partition);
      termsense.openRead();
      Document doc = new Document();
      doc.partition = partition;
      while (termsense.next()) {
         doc.docid = termsense.docid;
         int pos[] = termsense.getValue(doc);
         termsense.
         log.info("%d %s", termsense.docid, ArrayTools.concat(pos));
      }
   }


}
