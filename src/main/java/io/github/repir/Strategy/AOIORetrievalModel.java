package io.github.repir.Strategy;

import io.github.repir.Repository.Term;
import io.github.repir.Repository.TopicAOI;
import io.github.repir.Repository.TopicAOI.Record;
import io.github.repir.Retriever.Retriever;
import io.github.repir.Strategy.Operator.TermAOIO;
import io.github.repir.tools.Lib.ArrayTools;
import io.github.repir.tools.Lib.Log;

/**
 * @author jeroen
 */
public class AOIORetrievalModel extends RetrievalModelExpander {

   public static Log log = new Log(AOIORetrievalModel.class);

   public AOIORetrievalModel(Retriever retriever) {
      super(retriever);
   }

   @Override
   public String expandQuery() {
      TopicAOI termsense = (TopicAOI) repository.getFeature(TopicAOI.class);

      StringBuilder sb = new StringBuilder();
      for (String t : query.query.split("\\s+")) {
         Term term = repository.getTerm(t);
         Record sense = termsense.read( query.getID(), term.getID());
         if (sense != null) {
            log.info("%s %d %s", t, term.getID(), ArrayTools.concat(sense.senseoccurrence));
            sb.append(" ").append(GraphRoot.reformulate(TermAOIO.class, t));
         } else {
            sb.append(" ").append( t );
         }
      }
      log.info("getQueryToRetrieve() %s", sb);
      return sb.deleteCharAt(0).toString();
   }
}
