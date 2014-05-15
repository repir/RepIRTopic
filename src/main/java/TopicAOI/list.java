package TopicAOI;

import io.github.repir.Repository.AOI;
import io.github.repir.Repository.AOI.Rule;
import io.github.repir.Repository.Repository;
import io.github.repir.Repository.Term;
import io.github.repir.Repository.TermCF;
import io.github.repir.Repository.TopicAOI;
import io.github.repir.Repository.TopicAOI.Record;
import io.github.repir.Retriever.Retriever;
import io.github.repir.tools.Lib.ArgsParser;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Lib.PrintTools;
import io.github.repir.tools.Stemmer.englishStemmer;
import java.util.ArrayList;
import util.*;

public class list {

   public static Log log = new Log(listSense.class);
   static Repository repository;

   public static void main(String[] args) {
      ArgsParser parsedargs = new ArgsParser(args, "configfile [topic] [term]");
      repository = new Repository(parsedargs.get("configfile"));
      Term term = repository.getTerm(parsedargs.get("term"));
      int topic = parsedargs.getInt("topic");
      Retriever retriever = new Retriever(repository);
      TopicAOI termsense = (TopicAOI) repository.getFeature(TopicAOI.class);
      if (term == null) {
         termsense.openRead();
         for (Record record : termsense.getKeys()) {
            term( record, repository.getTerm(record.term));
         }
      } else {
         Record sense = termsense.read(topic, term.getID());
         term(sense, term);
      }
   }
   
   public static void term( Record sense, Term term ) {
      AOI aoi = (AOI)repository.getFeature(AOI.class, term.getProcessedTerm());
      ArrayList<Rule> rules = aoi.readRules();
      TermCF termtf = (TermCF)repository.getFeature(TermCF.class);
      termtf.openRead();
      long cf = termtf.readValue( sense.term );
      StringBuilder sb = new StringBuilder();
      sb.append("topic ").append( sense.topic ).append("term ").append( sense.term );
      for (Rule r : rules) {
         if (sense.senseoccurrence[r.sense] > 0) {
         double p_aoi_r = sense.senseoccurrence[r.sense] / (double)sense.cf;
         double p_r = sense.cf / (double)cf;
         double p_aoi = r.cf / (double)cf;
         double p_context = sense.cf / (double)cf;
         double p_aoi_context = sense.senseoccurrence[r.sense] / (double)r.cf;
         double p_r_aoi = (cf - 
                    (cf - r.cf) * 
                    ( p_aoi_context - p_context) / (1 - p_context)) / (double)repository.getCF();
            sb.append(PrintTools.sprintf("\n %d %d %d %d %s p(r|c)=%f", 
                    sense.senseoccurrence[r.sense],
                    sense.cf,
                    r.cf,
                    cf,
                    r.toString(repository),
                    p_r_aoi));
         }
      }
      log.printf("%s", sb.toString());
   }


}
