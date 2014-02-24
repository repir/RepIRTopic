package io.github.repir.Strategy;

import io.github.repir.Strategy.Term;
import io.github.repir.Strategy.GraphRoot;
import io.github.repir.Strategy.GraphNode;
import java.util.ArrayList;
import io.github.repir.Repository.AOI;
import io.github.repir.Repository.AOI.Rule;
import io.github.repir.Retriever.Document;
import io.github.repir.tools.Lib.Log;
import io.github.repir.Repository.TermInvertedSense;
import io.github.repir.Repository.TermInvertedSense.SensePos;
import io.github.repir.Repository.TopicAOI;
import io.github.repir.Repository.TopicAOI.Record;
import io.github.repir.tools.Lib.ArrayTools;
import io.github.repir.tools.Lib.MathTools;

/**
 * A Term scores the occurrences of a single term in a document.
 * <p/>
 * @author jeroen
 */
public class TermAOI extends Term {

   public static Log log = new Log(TermAOI.class);
   public TermInvertedSense storefeature;
   public double prel[];
   public double pc[];
   public double alpha;
   boolean contextfound;
   double maxrel = 0;

   public TermAOI(GraphRoot root, String originalterm, String stemmedterm) {
      super(root, originalterm, stemmedterm);
      alpha = repository.getConfigurationDouble("aoi.alpha", 0.8);
   }

   public TermAOI(GraphRoot im, ArrayList<Term> list) {
      this(im, list.get(0).originalterm, list.get(0).stemmedterm);
   }

   @Override
   public void prepareRetrieval() {
      storefeature = (TermInvertedSense) root.retrievalmodel.requestFeature("TermInvertedSense:" + channel + ":" + stemmedterm);
      storefeature.setTerm(stemmedterm);
   }

   @Override
   public void readStatistics() {
      TopicAOI termsense = (TopicAOI) repository.getFeature("TopicAOI");
      Record sense = termsense.read(retrievalmodel.query.id, termid);
      prel = new double[65];
      pc = new double[65];
      ArrayTools.fill(prel, 0);
      AOI aoi = (AOI) repository.getFeature("AOI");
      aoi.openRead();
      contextfound = false;
      log.info("termid %d", termid);
      ArrayList<Rule> rules = aoi.read(termid);

      double notp = 1;
      int count = 0;
      for (Rule r : rules) {
         double p_aoi_r = sense.senseoccurrence[r.sense] / (double) sense.cf;
         double p_context = sense.cf / (double) featurevalues.corpusfrequency;
         double p_aoi_context = sense.senseoccurrence[r.sense] / (double) r.cf;
         if (p_aoi_context - p_context > 0) {
            prel[r.sense] = sense.senseoccurrence[r.sense] / (double) sense.cf;
            contextfound = true;
         } else {
            prel[r.sense] = 0;
         }
      }
   }

   @Override
   public void process(Document doc) {
      double ptc = featurevalues.corpusfrequency / (double) repository.getCorpusTF();
      featurevalues.frequency = 0;
      double score = 0;
      SensePos value = storefeature.getValue(doc);
      featurevalues.pos = value.pos;
      if (featurevalues.pos == null) {
      } else if (!contextfound) {
         score = featurevalues.pos.length / (2500 * ptc);
      } else {
         for (long s : value.sense) {
            double minprel = 1;
            int parts = MathTools.numberOfSetBits(s) + 1;
            score += (1.0 / (double)parts) / (2500 * ptc);
            int bit = 0;
            while (s != 0) {
               if ((s & 1) == 1) {
                  score += (1.0 / (double)parts) / (2500 * prel[bit]);
               }
               bit++;
               s >>>= 1;
            }
         }
      }
      doc.score += featurevalues.queryweight * Math.log(1 + score);
   }

   @Override
   public GraphNode clone(GraphRoot newmodel) {
      if (termid >= 0) {
         TermAOI e = new TermAOI(newmodel, originalterm, stemmedterm);
         return e;
      } else {
         return null;
      }
   }

   @Override
   public String postReform() {
      StringBuilder sb = new StringBuilder();
      sb.append(getName()).append(":(");
      sb.append(stemmedterm).append(" ");
      sb.append(")").toString();
      return sb.toString();
   }

   @Override
   public String postReformUnweighted() {
      return postReform();
   }
}
