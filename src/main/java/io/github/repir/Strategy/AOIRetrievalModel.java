package io.github.repir.Strategy;

import io.github.repir.Repository.TopicAOI;
import io.github.repir.Repository.TopicAOI.Record;
import io.github.repir.Strategy.RetrievalModelExpander;
import io.github.repir.Strategy.GraphRoot;
import io.github.repir.Retriever.Retriever;
import io.github.repir.Retriever.Query;
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
   public String getQueryToRetrieve() {
      TopicAOI termsense = (TopicAOI) repository.getFeature("TopicAOI");

      StringBuilder sb = new StringBuilder();
      retriever.tokenizeQuery(query);
      for (String t : query.stemmedquery.split("\\s+")) {
         int termid = repository.termToID(t);
         Record sense = termsense.read( query.id, termid);
         if (sense != null) {
            log.info("%s %d %s", t, termid, ArrayTools.toString(sense.senseoccurrence));
            sb.append(" ").append(GraphRoot.reformulate(TermAOI.class, t));
         } else {
            sb.append(" ").append( t );
         }
      }
      return sb.deleteCharAt(0).toString();
   }
}
