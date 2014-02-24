package io.github.repir.Strategy;

import java.util.Collection;
import java.util.Map;
import io.github.repir.Retriever.Retriever;
import io.github.repir.Repository.TermContext.Sample;
import io.github.repir.tools.DataTypes.HashMap;
import io.github.repir.tools.Lib.Log;

/**
 * This feature caches the context of all occurrences of a term in the
 * collection. When cached, this feature can be used by other features to
 * analyze the local context a term appears in.
 * <p/>
 * @author jeroen
 */
public abstract class AOIAnalyzer1 extends AOIAnalyzer {

   public static Log log = new Log(AOIAnalyzer1.class);

   public AOIAnalyzer1(Retriever retriever) {
      super(retriever);
   }
   
   public HashMap<Integer, Double> bagOfWords(Collection<Sample1> set) {
      HashMap<Integer, Double> bow = new HashMap<Integer, Double>();
      for (Sample s : set) {
         for (int w : s.leftcontext) {
            if (!stopwords.contains(w)) {
               Double c = bow.get(w);
               if (c == null) {
                  bow.put(w, 1.0);
               } else {
                  bow.put(w, c + 1.0);
               }
            }
         }
         for (int w : s.rightcontext) {
            if (!stopwords.contains((w))) {
               Double c = bow.get(w);
               if (c == null) {
                  bow.put(w, 1.0);
               } else {
                  bow.put(w, c + 1);
               }
            }
         }
      }
      double sum = 0;
      for (Map.Entry<Integer, Double> e : bow.entrySet()) {
         TermStats s = terms.get(e.getKey());
         double tfidf = e.getValue() * s.idf;
         bow.put(e.getKey(), tfidf);
         sum += tfidf;
      }
      for (Map.Entry<Integer, Double> e : bow.entrySet()) {
         bow.put(e.getKey(), e.getValue() / sum);
      }
      return bow;
   }
}
