package io.github.repir.Strategy.Tools;

import io.github.repir.Strategy.FeatureValues;
import io.github.repir.Strategy.GraphNode;
import io.github.repir.Repository.DocTF;
import io.github.repir.Repository.Repository;
import io.github.repir.Strategy.Tools.ScoreFunction;
import io.github.repir.Retriever.Document;
import io.github.repir.Strategy.FeatureValues;
import io.github.repir.Strategy.FeatureValues;
import io.github.repir.Strategy.GraphNode;
import io.github.repir.Strategy.GraphNode;
import io.github.repir.Strategy.Tools.ScoreFunction;
import io.github.repir.tools.Lib.Log;

/**
 * Implementation of Zhai and Lafferty (2001) algorithm to minimize
 * KL-divergence between a query language model and a Dirichlet smoothed
 * document language model.
 * <p/>
 * mu is the Dirichlet prior, which in theory resembles the average length of
 * documents in the corpus, and can be modified by setting LM.mu in the
 * configuration file.
 * <p/>
 * @author jeroen
 */
public class QL2ScoreFunction extends ScoreFunction<QL2ScoreFunction.Scorable> {

   public static Log log = new Log(QL2ScoreFunction.class);
   public int mu = 2500;
   DocTF doctf;

   public QL2ScoreFunction(Repository repository) {
      super(repository);
   }

   public void prepareRetrieve() {
      mu = repository.getConfigurationInt("kld.mu", 2500);
      doctf = (DocTF) retrievalmodel.requestFeature("DocTF:all");
   }

   @Override
   public Scorable create(GraphNode feature) {
      return new Scorable(feature);
   }

   @Override
   public double score(Document doc) {
      score = 0;
      double alpha = mu / (double) (mu + doctf.getValue());
      for (Scorable scorable : scorables) {
         FeatureValues values = scorable.feature.getFeatureValues();
         if (values != null) {
            double frequency = values.frequency;
            double featurescore = scorable.queryweight * Math.log((1 - alpha) * frequency / doctf.getValue() + alpha * (scorable.ptc));

            if (report) {
               if (Double.isNaN(score)) {
                  doc.addReport("[%s] NaN ptc=%f", scorable.feature.toTermString(), scorable.ptc);
               } else {
                  doc.addReport("[%s] freq=%f ptc=%f score=%f", scorable.feature.toTermString(), frequency, scorable.ptc, featurescore);
               }
            }
            if (!Double.isNaN(score)) {
               score += featurescore;
            }
         }
      }
      return score;
   }

   public class Scorable extends ScoreFunction.Scorable {

      double ptc;
      double queryweight;

      public Scorable(GraphNode feature) {
         super(feature);
         ptc = feature.getFeatureValues().corpusfrequency / (double) repository.getCorpusTF();
         queryweight = feature.getFeatureValues().queryweight;
      }
   }
}
