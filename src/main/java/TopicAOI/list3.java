package TopicAOI;

import util.*;
import java.util.ArrayList;
import io.github.repir.Repository.Repository;
import io.github.repir.Retriever.Retriever;
import io.github.repir.Repository.AOI;
import io.github.repir.Repository.AOI.Rule;
import io.github.repir.Repository.TermDF;
import io.github.repir.Repository.TermTF;
import io.github.repir.Repository.TopicAOI;
import io.github.repir.Repository.TopicAOI.Record;
import io.github.repir.tools.Lib.ArgsParser;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Lib.PrintTools;
import io.github.repir.tools.Stemmer.englishStemmer;

public class list3 {

   public static Log log = new Log(listSense.class);
   static Repository repository;

   public static void main(String[] args) {
      ArgsParser parsedargs = new ArgsParser(args, "configfile [topic] [term]");
      repository = new Repository(parsedargs.get("configfile"));
      String term = parsedargs.get("term");
      int topic = parsedargs.getInt("topic");
      Retriever retriever = new Retriever(repository);
      TopicAOI termsense = (TopicAOI) repository.getFeature("TopicAOI");
      if (term == null) {
         termsense.openRead();
         for (Record record : termsense.getKeys()) {
            term( record);
         }
      } else {
         int termid = repository.termToID(englishStemmer.get().stem(term));
         Record sense = termsense.read(topic, termid);
         term(sense);
      }
   }
   
   public static void term( Record sense ) {
      AOI aoi = (AOI)repository.getFeature("AOI");
      aoi.openRead();
      ArrayList<Rule> rules = aoi.read( sense.term );
      TermTF termtf = (TermTF)repository.getFeature("TermTF");
      termtf.openRead();
      long cf = termtf.readValue( sense.term );
      TermDF termdf = (TermDF)repository.getFeature("TermDF");
      termdf.openRead();
      long df = termdf.readValue( sense.term );
      StringBuilder sb = new StringBuilder();
      sb.append("topic ").append( sense.topic ).append("term ").append( sense.term );
      for (Rule r : rules) {
         double p_aoi_c = sense.sensedf[r.sense] / (double) sense.df;
         double p_aoi_d = r.df / (double) df;
            double p_aoi_context = p_aoi_c - p_aoi_d;
            if (p_aoi_context < 0)
               p_aoi_context = 0;
         if (p_aoi_context > 0) {

            sb.append(PrintTools.sprintf("\n %d %d %d %d %s p(r|c)=%f", 
                    sense.sensedf[r.sense],
                    sense.df,
                    r.df,
                    df,
                    r.toString(repository),
                    p_aoi_context));
         }
      }
      log.printf("%s", sb.toString());
   }


}
