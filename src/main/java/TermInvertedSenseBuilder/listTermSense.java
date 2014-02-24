package TermInvertedSenseBuilder;

import util.*;
import java.util.ArrayList;
import io.github.repir.Repository.Repository;
import io.github.repir.Retriever.Retriever;
import io.github.repir.Repository.AOI;
import io.github.repir.Repository.AOI.Rule;
import io.github.repir.Repository.TermInvertedSense;
import io.github.repir.Retriever.Document;
import io.github.repir.tools.Lib.ArgsParser;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Stemmer.englishStemmer;

public class listTermSense {

   public static Log log = new Log(listSense.class);

   public static void main(String[] args) {
      englishStemmer stemmer = englishStemmer.get();
      ArgsParser parsedargs = new ArgsParser(args, "configfile partition term");
      Repository repository = new Repository(parsedargs.get("configfile"));
      int partition = parsedargs.getInt("partition");
      String stemmedterm = stemmer.stem(parsedargs.get("term"));
      Retriever retriever = new Retriever(repository);
      TermInvertedSense termsense = (TermInvertedSense) repository.getFeature("TermInvertedSense:all:" + 
            stemmedterm  );
      termsense.setTerm(stemmedterm);
      termsense.setPartition(partition);
      termsense.openRead();
      Document doc = new Document();
      doc.partition = partition;
      while (termsense.next()) {
         doc.docid = termsense.docid;
         TermInvertedSense.SensePos value = termsense.getValue(doc);
         log.info("%d %s", termsense.docid, value.toString());
      }
   }


}
