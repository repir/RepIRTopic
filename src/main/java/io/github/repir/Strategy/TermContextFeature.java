package io.github.repir.Strategy;

import io.github.repir.Strategy.GraphNodeCachable;
import io.github.repir.Strategy.Term;
import io.github.repir.Strategy.GraphRoot;
import io.github.repir.Strategy.GraphNode;
import io.github.repir.Strategy.Collector.TermContextCollector;
import io.github.repir.Repository.TermContext;
import java.util.ArrayList;
import java.util.HashMap;
import io.github.repir.Repository.DocForward;
import io.github.repir.Repository.Repository;
import io.github.repir.Repository.TermContext.Record;
import io.github.repir.Retriever.Document;
import io.github.repir.Repository.TermContext.Doc;
import io.github.repir.Repository.TermContext.Sample;
import io.github.repir.tools.Lib.Log;

/**
 * This feature caches the context of all occurrences of a term in the collection. When cached, this
 * feature can be used by other features to analyze the local context a term appears in.
 * <p/>
 * @author jeroen
 */
public class TermContextFeature extends GraphNodeCachable {

   public static Log log = new Log(TermContextFeature.class);
   public DocForward docforward;
   public TermContext termcontext;
   public Term term;
   public TermContextCollector collector;
   public HashMap<Doc, ArrayList<Sample>> samples;

   public TermContextFeature(GraphRoot root) {
      super(root);
   }

   public TermContextFeature(GraphRoot root, ArrayList<Term> list) {
      this(root);
      if (list.size() != 1) {
         log.fatal("Can only use TermContextFeature on a single term");
      }
      for (Term t : list) {
         add(t);
         term = t;
      }
   }
   
   public TermContextFeature(Repository repository, Term term) {
      super(repository);
      this.term = term;
   }

   public void prepareRetrieval() {
      docforward = (DocForward) retrievalmodel.requestFeature("DocForward:all");
   }
   
   public void doAnnounce() {
         super.announce(ANNOUNCEKEY.NEEDSCACHECOLLECT, this);
   }

   public void configureFeatures() { }
   
   /**
    * If applicable, simplifies the GraphRoot by replacing 1-word phrases by a PhraseTerm. Also, if
    * the span is smaller than the minimal span of an occurrence, this is adjusted.
    */
   @Override
   public void readStatistics() {
      collector = new TermContextCollector(this);
   }

   public TermContext getCache() {
      if (termcontext == null) {
         termcontext = (TermContext) repository.getFeature("TermContext");
      }
      return termcontext;
   }

   @Override
   public void process(Document doc) {
      //log.info("process %d", doc.docid);
      term.process(doc);
   }

   // Lazy implementation, unlikely we'll ever clone this
   @Override
   public GraphNode clone(GraphRoot newmodel) {
      return null;
   }

   @Override
   public void readCachedData() {
      getCache();
      this.samples = this.termcontext.read(term.termid);
      if (samples != null)
         root.remove(this);
   }
   
}
