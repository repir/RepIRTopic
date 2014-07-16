package TermInvertedSenseBuilder;

import io.github.repir.Repository.AOI;
import io.github.repir.Repository.AOI.Rule;
import io.github.repir.Repository.Repository;
import io.github.repir.Repository.Term;
import io.github.repir.Repository.TermInvertedSense;
import io.github.repir.Retriever.Document;
import io.github.repir.Retriever.Retriever;
import io.github.repir.tools.Lib.ArgsParser;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Stemmer.englishStemmer;
import java.util.ArrayList;
import util.*;

public class listTermSenseDoc {

   public static Log log = new Log(listSense.class);

   public static void main(String[] args) {
      ArgsParser parsedargs = new ArgsParser(args, "configfile docid partition term");
      Repository repository = new Repository(parsedargs.get("configfile"));
      int docid = parsedargs.getInt("docid");
      int partition = parsedargs.getInt("partition");
      Term term = repository.getTerm(parsedargs.get("term"));
      Retriever retriever = new Retriever(repository);
      AOI aoi = (AOI) repository.getFeature(AOI.class, term.getProcessedTerm());
      TermInvertedSense termsense = (TermInvertedSense) repository.getFeature(TermInvertedSense.class, "all", term.getProcessedTerm());
      termsense.setTerm(term);
      termsense.setPartition(partition);
      termsense.openRead();
      Document doc = new Document();
      doc.partition = partition;
      doc.docid = docid;
      while (termsense.next()) {
         if (termsense.docid == doc.docid) {
            TermInvertedSense.SensePos value = termsense.getValue(doc);
            log.info("%d %s", termsense.docid, value.toString());
            
            long combined = 0;
            for (long s : value.sense)
               combined |= s;
            ArrayList<Rule> rules = aoi.readRules();
            for (Rule r : rules)
               if (((1l << r.sense) & combined) != 0)
                  log.printf("%s", r.toString(repository));
         }
      }
   }
}
