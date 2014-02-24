package io.github.repir.Strategy;

import io.github.repir.Strategy.RetrievalModelAnalyze;
import io.github.repir.Strategy.GraphRoot;
import io.github.repir.Retriever.Retriever;
import io.github.repir.Retriever.Query;
import io.github.repir.tools.Lib.Log;

/**
 * @author jeroen
 */
public class ContextRetrievalModel extends RetrievalModelAnalyze {

   public static Log log = new Log(ContextRetrievalModel.class);

   public ContextRetrievalModel(Retriever retriever) {
      super(retriever);
   }

   @Override
   public String getQueryToRetrieve() {
      StringBuilder sb = new StringBuilder();
      for (String t : query.stemmedquery.split("\\s+")) {
         sb.append(" ").append(GraphRoot.reformulate(TermContextFeature.class, query.stemmedquery));
      }
      return sb.deleteCharAt(0).toString();
   }

   @Override
   public Query finishReduceTask() {
      query.setStrategyClass(null);
      return query;
   }
}
