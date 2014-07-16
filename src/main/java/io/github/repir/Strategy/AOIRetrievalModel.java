package io.github.repir.Strategy;

import io.github.repir.Repository.Term;
import io.github.repir.Repository.TopicAOI;
import io.github.repir.Repository.TopicAOI.Record;
import io.github.repir.Retriever.Retriever;
import io.github.repir.Strategy.Operator.TermAOI;
import io.github.repir.tools.Lib.ArrayTools;
import io.github.repir.tools.Lib.Log;

/**
 * @author jeroen
 */
public class AOIRetrievalModel extends RetrievalModelExpander {

   public static Log log = new Log(AOIRetrievalModel.class);

   public AOIRetrievalModel(Retriever retriever) {
      super(retriever);
   }

   @Override
   public String expandQuery() {
      TopicAOI termsense = (TopicAOI) repository.getFeature(TopicAOI.class);

      StringBuilder sb = new StringBuilder();
      for (String t : query.query.split("\\s+")) {
         Term term = repository.getTerm(t);
         Record sense = termsense.read( query.id, term.getID());
         if (sense != null) {
            log.info("%s %d %s", t, term.getID(), ArrayTools.concat(sense.senseoccurrence));
            sb.append(" ").append(GraphRoot.reformulate(TermAOI.class, t));
         } else {
            sb.append(" ").append( t );
         }
      }
      return sb.deleteCharAt(0).toString();
   }
}
