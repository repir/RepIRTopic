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
import io.github.repir.tools.Lib.ArrayTools;
import io.github.repir.tools.Lib.Log;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * A Term scores the occurrences of a single term in a document.
 * <p/>
 * @author jeroen
 */
public class TermAOIO extends QTerm {

   public static Log log = new Log(TermAOIO.class);
   private double alpha;
   public double prel[];
   boolean contextfound;
   double maxrel = 0;
   
   public TermAOIO(GraphRoot root, Term term) {
      super(root, term);      
   }
   
   public TermAOIO(GraphRoot im, ArrayList<QTerm> list) {
      this(im, list.get(0).term);
   }

   @Override
   public void prepareRetrieval() {
      storefeature = (TermInvertedSense) root.retrievalmodel.requestFeature(
              TermInvertedSense.class, channel, term.getProcessedTerm());
      storefeature.setTerm(term);
      alpha = root.repository.configuredDouble("aoi.alpha", 0.5);
   }
   
   @Override
   public void readStatistics() {
      super.readStatistics();
      TopicAOI termsense = (TopicAOI) repository.getFeature(TopicAOI.class);
      Record sense = termsense.read(retrievalmodel.query.getID(), term.getID());
      log.info("readStatistics %s id %d %s cf %d df %d prior %f weight %f", term.getProcessedTerm(), term.getID(), sense, cf, df, documentprior, queryweight);
      prel = new double[65];
      ArrayTools.fill(prel, 0);
      AOI aoi = (AOI) repository.getFeature(AOI.class, term.getProcessedTerm());
      aoi.openRead();
      contextfound = false;
      ArrayList<Rule> rules = aoi.readRules();
      int total = 0;
      HashMap<Integer, Rule> map = new HashMap<Integer, Rule>();
      for (Rule r : rules) {
         map.put(r.sense, r);
         total += r.cf;
      }
      for (Rule r : rules) {
         double unex = sense.senseoccurrence[r.sense] / (double)sense.cf;
         if (unex > 0) {
            log.info("Rule %s %s %f", term.getProcessedTerm(), r.toString(repository), unex);
            prel[r.sense] = unex;
            contextfound = true;
         }
      }
   }
   
   @Override
   public void process(Document doc) {
      SensePos value = (SensePos)storefeature.getValue(doc);
      pos = value.pos;
      setFrequency(pos != null?pos.length:0);
      if (pos != null && pos.length > 0) {
         double pinterest = 1;
         for (long s : value.sense) {
            if (s == 0) {
               pinterest *= (1-prel[64]);
               //featurevalues.cf += (prel[64]);
            } else {
               int bit = 0;
               while (s != 0) {
                  if ((s & 1) == 1) {
                     pinterest *= (1-prel[bit]);
                  }
                  bit++;
                  s >>>= 1;
               }
            }
         }
         frequency -= alpha * pos.length * (pinterest);
      }
   }

   @Override
   public Operator clone(GraphRoot newmodel) {
      if (term.exists()) {
         TermAOIO e = new TermAOIO(newmodel, term);
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
