package io.github.repir.Strategy.Operator;

import io.github.repir.Repository.AOI;
import io.github.repir.Repository.AOI.Rule;
import io.github.repir.Repository.Term;
import io.github.repir.Repository.TermInvertedSense;
import io.github.repir.Repository.TermInvertedSense.SensePos;
import io.github.repir.Repository.TopicAOI;
import io.github.repir.Repository.TopicAOI.Record;
import io.github.repir.Retriever.Document;
import io.github.repir.Strategy.GraphRoot;
import io.github.repir.Strategy.GraphRoot;
import io.github.repir.Strategy.Operator.Operator;
import io.github.repir.Strategy.Operator.QTerm;
import io.github.repir.tools.Lib.ArrayTools;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Lib.MathTools;
import java.util.ArrayList;

/**
 * A Term scores the occurrences of a single term in a document.
 * <p/>
 * @author jeroen
 */
public class TermAOI extends QTerm {

   public static Log log = new Log(TermAOI.class);
   public TermInvertedSense storefeature;
   public double prel[];
   public double pc[];
   public double alpha;
   boolean contextfound;
   double maxrel = 0;

   public TermAOI(GraphRoot root, Term term) {
      super(root, term);
      alpha = repository.configuredDouble("aoi.alpha", 0.8);
   }

   public TermAOI(GraphRoot im, ArrayList<QTerm> list) {
      this(im, list.get(0).term);
   }

   @Override
   public void prepareRetrieval() {
      storefeature = (TermInvertedSense) root.retrievalmodel.requestFeature(
              TermInvertedSense.class, channel, term.getProcessedTerm());
      storefeature.setTerm(term);
   }

   @Override
   public void readStatistics() {
      super.readStatistics();
      TopicAOI termsense = (TopicAOI) repository.getFeature(TopicAOI.class);
      Record sense = termsense.read(retrievalmodel.query.id, term.getID());
      prel = new double[65];
      pc = new double[65];
      ArrayTools.fill(prel, 0);
      AOI aoi = (AOI) repository.getFeature(AOI.class, term.getProcessedTerm());
      aoi.openRead();
      contextfound = false;
      log.info("termid %d", term.getID());
      ArrayList<Rule> rules = aoi.readRules();

      double notp = 1;
      int count = 0;
      for (Rule r : rules) {
         double p_aoi_r = sense.senseoccurrence[r.sense] / (double) sense.cf;
         double p_context = sense.cf / (double) getCF();
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
      double ptc = getCF() / (double) repository.getCF();
      setFrequency(0);
      double score = 0;
      SensePos value = storefeature.getValue(doc);
      pos = value.pos;
      if (pos == null) {
      } else if (!contextfound) {
         score = pos.length / (2500 * ptc);
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
      doc.score += getQueryWeight() * Math.log(1 + score);
   }

   @Override
   public Operator clone(GraphRoot newmodel) {
      if (term.exists()) {
         TermAOI e = new TermAOI(newmodel, term);
         return e;
      } else {
         return null;
      }
   }

   @Override
   public String postReform() {
      StringBuilder sb = new StringBuilder();
      sb.append(getName()).append(":(");
      sb.append(term.toString()).append(" ");
      sb.append(")").toString();
      return sb.toString();
   }

   @Override
   public String postReformUnweighted() {
      return postReform();
   }
}
