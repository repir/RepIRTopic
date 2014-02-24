package util;

import util.*;
import java.util.ArrayList;
import io.github.repir.Repository.Repository;
import io.github.repir.Retriever.Retriever;
import io.github.repir.Repository.AOI;
import io.github.repir.Repository.AOI.Rule;
import io.github.repir.Repository.DocForward;
import io.github.repir.Repository.TermInverted;
import io.github.repir.Repository.TermInvertedSense;
import io.github.repir.Retriever.Document;
import io.github.repir.tools.Lib.ArgsParser;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Stemmer.englishStemmer;

public class matchAOI {

   public static Log log = new Log(listSense.class);

   public static void main(String[] args) {
      englishStemmer stemmer = englishStemmer.get();
      ArgsParser parsedargs = new ArgsParser(args, "configfile docid partition term");
      Repository repository = new Repository(parsedargs.get("configfile"));
      int docid = parsedargs.getInt("docid");
      int partition = parsedargs.getInt("partition");
      String stemmedterm = stemmer.stem(parsedargs.get("term"));
      Retriever retriever = new Retriever(repository);
      DocForward forward = (DocForward)repository.getFeature("DocForward:all");
      TermInverted postinglist = (TermInverted)repository.getFeature("TermInverted:all:" + stemmedterm);
      postinglist.setPartition(partition);
      postinglist.setTerm(stemmedterm);
      postinglist.setBufferSize(4096 * 10000);
      postinglist.openRead();
      AOI.RuleSet rules = new AOI.RuleSet(repository, postinglist.termid);
      Document doc = new Document();
      doc.partition = partition;
      doc.docid = docid;
      while (postinglist.next()) {
         if (postinglist.docid == doc.docid) {
            int pos[] = postinglist.getValue(doc);
            forward.read(doc);
            int content[] = forward.getValue();
            long sense[] = rules.matchAll(content, pos);
            long combined = 0;
            for (long s : sense)
               combined |= s;
            for (Rule r : rules)
               if (((1l << r.sense) & combined) != 0)
                  log.printf("%s", r.toString(repository));
         }
      }
   }
}
