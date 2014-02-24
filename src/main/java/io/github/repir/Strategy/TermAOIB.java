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
import io.github.repir.tools.DataTypes.HashMap;
import io.github.repir.tools.Lib.ArrayTools;

/**
 * A Term scores the occurrences of a single term in a document.
 * <p/>
 * @author jeroen
 */
public class TermAOIB extends Term {

   public static Log log = new Log(TermAOIB.class);
   public TermInvertedSense storefeature;
   public double prel[];
   public double alpha;
   boolean contextfound;
   double maxrel = 0;
   
   public TermAOIB(GraphRoot root, String originalterm, String stemmedterm) {
      super(root, originalterm, stemmedterm);      
      alpha = repository.getConfigurationDouble("aoi.alpha", 1.0);
   }
   
   public TermAOIB(GraphRoot im, ArrayList<Term> list) {
      this(im, list.get(0).originalterm, list.get(0).stemmedterm);
   }

   @Override
   public void prepareRetrieval() {
      //log.info("prepareRetrieval() %s", this.stemmedterm);
      featurevalues.documentprior = featurevalues.queryweight;
      storefeature = (TermInvertedSense) root.retrievalmodel.requestFeature("TermInvertedSense:"+channel+":" + stemmedterm);
      storefeature.setTerm(stemmedterm);
      TopicAOI termsense = (TopicAOI) repository.getFeature("TopicAOI");
      Record sense = termsense.read(retrievalmodel.query.id, termid);
      prel = new double[65];
      ArrayTools.fill(prel, 0);
      AOI aoi = (AOI) repository.getFeature("AOI");
      aoi.openRead();
      contextfound = false;
      //log.info("termid %d", termid);
      ArrayList<Rule> rules = aoi.read(termid);

      for (Rule r : rules) {
         double p_aoi_c = sense.sensedf[r.sense] / (double)sense.df;
         double p_aoi_r = sense.sensedoccurrence[r.sense] / (double) r.cf;
         double p_r = sense.cf / (double) featurevalues.corpusfrequency;
         double p_aoi_d = r.df / (double) featurevalues.documentfrequency;
            prel[r.sense] = (p_aoi_c - p_aoi_d);
            if (prel[r.sense] < 0)
               prel[r.sense] = 0;
            contextfound = true;
      }
   }
   
   @Override
   public void process(Document doc) {
      //log.info("process term %s doc %d storefeature %s", this.stemmedterm, doc.docid, storefeature);
      SensePos value = storefeature.getValue(doc);
      featurevalues.pos = value.pos;
      if (featurevalues.pos == null) {
         featurevalues.frequency = 0;
      } else if (!contextfound) {
         featurevalues.frequency = featurevalues.pos.length;
      } else {
         featurevalues.frequency = 0;
         featurevalues.frequency = featurevalues.pos.length;
         long sense = 0;
         for (long s : value.sense) {
            sense |= s;
         }
            double pinterest = 1;
            if (sense == 0) {
               
            } else {
               int bit = 0;
               while (sense != 0) {
                  if ((sense & 1) == 1) {
                     pinterest *= (1-prel[bit]);
                  }
                  bit++;
                  sense >>>= 1;
               }
               featurevalues.frequency += alpha * featurevalues.pos.length * (1 - pinterest);
            }
      }
   }

   @Override
   public GraphNode clone(GraphRoot newmodel) {
      if (termid >= 0) {
         TermAOIB e = new TermAOIB(newmodel, originalterm, stemmedterm);
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
   
   @Override
   public boolean equals(Object o) {
      return this == o;
   }
}
