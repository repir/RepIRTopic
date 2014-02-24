package io.github.repir.Strategy;

import io.github.repir.Repository.TermContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import io.github.repir.Retriever.Retriever;
import io.github.repir.Repository.TermContext.Doc;
import io.github.repir.Repository.TermContext.Sample;
import io.github.repir.Repository.AOI.Rule;
import io.github.repir.Repository.AOI.RuleType;
import static io.github.repir.Repository.AOI.RuleType.LEFT2;
import static io.github.repir.Repository.AOI.RuleType.RIGHT2;
import io.github.repir.tools.DataTypes.HashMap;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Lib.MathTools;

/**
 * This feature caches the context of all occurrences of a term in the
 * collection. When cached, this feature can be used by other features to
 * analyze the local context a term appears in.
 * <p/>
 * @author jeroen
 */
public class AOIAnalyzer5 extends AOIAnalyzer1 {

   public static Log log = new Log(AOIAnalyzer5.class);
   public ArrayList<Cluster> clusters = new ArrayList<Cluster>();
   public HashMap<Rule1, Rule1> rulesmap = new HashMap<Rule1, Rule1>();
   public int samplesize;
   public TreeSet<Winner> winners;
   public ArrayList<Rule1> rules = new ArrayList<Rule1>();
   public final int width;

   public AOIAnalyzer5(Retriever retriever) {
      super(retriever);
      width = retriever.repository.getConfigurationInt("aoi.width", 10);
      MININNERSIM = retriever.repository.getConfigurationDouble("aoi.sim", 0.35);
      MAXOVERLAP = 1 - retriever.repository.getConfigurationDouble("aoi.novel", 0.10);
   }

   public void readCachedData() {
      termcontext = (TermContext) repository.getFeature("TermContext");
      java.util.HashMap<Doc, ArrayList<Sample>> docsamples = termcontext.read(term.termid);
      for (Map.Entry<Doc, ArrayList<Sample>> e : docsamples.entrySet()) {
         for (Sample s : e.getValue()) {
            samples.add(new Sample1(s.pos, e.getKey().docid, e.getKey().partition, s.leftcontext, s.rightcontext));
         }
      }
      termcontext = null;
      samplesize = samples.size();
      buildTermList();
   }
   double MININNERSIM = 0.37;
   int MINSAMPLESIZE = 20;
   double MAXOVERLAP;
   double usefulnessthreshold = 0;
   double uniquethreshold = 0.1;
   int MAXSENSE = 128;

   @Override
   public void doMapTask() {
      int sense = 0;
      log.info("term %s", term.stemmedterm);
      log.startTime();
      readCachedData();
      log.reportTime("Data read samples=%d", samplesize);
      MINSAMPLESIZE = 5; //Math.max(5, (int) Math.pow(samplesize, 0.25));
      generateNewRules(samples);
      log.reportTime("rules generated %d", rules.size());
      pruneSmallRules();
      log.reportTime("rules pruned=%d", rules.size());
      winners = new TreeSet<Winner>();
      //printFreq();

      HashSet<Sample1> covered = new HashSet<Sample1>();
      TreeSet<Rule1> sorted = new TreeSet<Rule1>(rules);
      int smallestwinner = -1;
      log.reportTime("sorted");

      Iterator<Rule1> iter = sorted.iterator();
      while (iter.hasNext() && winners.size() < MAXSENSE * 8) {
         Rule1 rule = iter.next();
         Cluster current = new Cluster(rule);
         //double overlap = current.overlap(covered);
         if (current.meaningful() > 0.3) {
            winners.add(new Winner(current));
            covered.addAll(current.getSamples());
            if (winners.size() > MAXSENSE * 8) {
               break;
            }
         } else {
            iter.remove();
         }
      }
      rules = null;
      log.reportTime("prune winners");
      sense = 0;
      group();
      pruneWinners();
      log.reportTime("winners pruned");
      covered = new HashSet<Sample1>();
      winners = new TreeSet<Winner>(winners);
      for (Winner w : winners) {
         if (w.overlap(covered) < MAXOVERLAP) {
            //printCluster(w);
            for (Rule1 r : w) {
               r.cf = w.getSamples().size();
               HashSet<Long> docids = new HashSet<Long>();
               for (Sample1 s : w.getSamples())
                  docids.add(s.docid);
               r.df = docids.size();
               if (sense < 64) {
                  collector.addRule(r.setSense(sense));
               }
            }
            log.info("samples %d cos %f overlap %f", w.getSamples().size(), w.innersim(), w.overlap(covered));
            log.info("%d %s\n", sense, w);
            covered.addAll(w.getSamples());
            sense++;
            if (sense >= MAXSENSE) {
               break;
            }
         }
      }
      Rule r = new Rule(MAXSENSE, term.termid, RuleType.NORULE, 0);
      r.cf = samplesize - covered.size();
      collector.addRule(r);
      log.reportTime("done");
   }

   public void pruneSmallRules() {
      Iterator<Rule1> iter = rules.iterator();
      while (iter.hasNext()) {
         Rule1 r = iter.next();
         if (r.getSamples().size() < MINSAMPLESIZE) {
            iter.remove();
         }
      }
   }
   
   public void group() {
      ArrayList<Winner> list = new ArrayList<Winner>(winners);
      winners = null;
      for (int i = list.size()-2; i >= 0; i--) {
         for (int j = list.size() - 1; j > i; j--) {
            Winner w = list.get(i);
            Winner v = list.get(j);
            if (w.overlap(v.getSamples()) > 0.5) {
            //if (w.getBow().overlap(v.getBow()) > 0.4) {
               Winner c = new Winner(w, v);
               if (c.compareTo(w) < 0) {
                  list.set(i, c);
                  if (c.compareTo(v) < 0) {
                    list.remove(j);
                  }
               } else if (c.compareTo(v) < 0) {
                  list.set(j, c);
               } 
            }
         }
      }
      winners = new TreeSet<Winner>(list);
   }

   public void pruneWinners() {
      HashSet<Sample1> covered = new HashSet<Sample1>();
      Iterator<Winner> iter = winners.iterator();
      while (iter.hasNext()) {
         Winner w = iter.next();
         if (w.innersim() < MININNERSIM) {
            iter.remove();
         } else if (w.overlap(covered) >= MAXOVERLAP) {
            iter.remove();
         } else {
            covered.addAll(w.getSamples());
         }
      }
   }

   public void addWinner(Cluster w) {
      winners.add(new Winner(w));
   }

   public boolean ruleMatch(Rule r, Sample s) {
      return r.match(s.leftcontext, s.rightcontext);
   }

   public void generateNewRules(ArrayList<Sample1> samples) {
      Rule1 r;

      //word to left
      for (Sample1 s : samples) {
         if (s.leftcontext.length > 0 && !stopwords.contains(s.leftcontext[0])) {
            r = new Rule1(RuleType.LEFT, s.leftcontext[0]);
            addRule(r, s);
         }
      }

      // word to right
      for (Sample1 s : samples) {
         if (s.rightcontext.length > 0 && !stopwords.contains(s.rightcontext[0])) {
            r = new Rule1(RuleType.RIGHT, s.rightcontext[0]);
            addRule(r, s);
         }
      }

      // word in window
      for (Sample1 s : samples) {
         for (int p = Math.min(width, s.leftcontext.length) - 1; p >= 0; p--) {
            int i = s.leftcontext[p];
            if (!stopwords.contains(i) && i != term.termid) {
               r = new Rule1(RuleType.WINDOW, i);
               addRule(r, s);
            }
         }
         for (int p = Math.min(width, s.rightcontext.length) - 1; p >= 0; p--) {
            int i = s.rightcontext[p];
            if (!stopwords.contains(i) && i != term.termid) {
               r = new Rule1(RuleType.WINDOW, i);
               addRule(r, s);
            }
         }
      }

      // word to LR
      for (Sample1 s : samples) {
         if (s.leftcontext.length > 0 && s.rightcontext.length > 0 && !stopwords.contains(s.rightcontext[0])) {
            r = new Rule1(RuleType.LR, s.leftcontext[0], s.rightcontext[0]);
            addRule(r, s);
         }
      }

      // 2 word WINDOW
      for (Sample1 s : samples) {
         for (int i = Math.min(width, s.leftcontext.length) - 2; i >= 0; i--) {
            if (!stopwords.contains(s.leftcontext[i + 1]) && !stopwords.contains(s.leftcontext[i])) {
               r = new Rule1(RuleType.WINDOW2, s.leftcontext[i + 1], s.leftcontext[i]);
               addRule(r, s);
            }
         }
         for (int i = Math.min(width, s.rightcontext.length) - 2; i >= 0; i--) {
            if (!stopwords.contains(s.rightcontext[i]) && !stopwords.contains(s.rightcontext[i + 1])) {
               r = new Rule1(RuleType.WINDOW2, s.rightcontext[i], s.rightcontext[i + 1]);
               addRule(r, s);
            }
         }
      }

      // 2 word to RIGHT
      for (Sample1 s : samples) {
         if (s.rightcontext.length > 1 && !stopwords.contains(s.rightcontext[1])) {
            r = new Rule1(RuleType.RIGHT2, s.rightcontext[0], s.rightcontext[1]);
            addRule(r, s);
         }
      }

      // 2 word to LEFT
      for (Sample1 s : samples) {
         if (s.leftcontext.length > 1 && !stopwords.contains(s.leftcontext[1])) {
            r = new Rule1(RuleType.LEFT2, s.leftcontext[1], s.leftcontext[0]);
            addRule(r, s);
         }
      }

      for (Sample1 s : samples) {
         if (s.leftcontext.length > 2 && !stopwords.contains(s.leftcontext[2])) {
            r = new Rule1(RuleType.LEFT3, s.leftcontext[2], s.leftcontext[1], s.leftcontext[0]);
            addRule(r, s);
         }
         if (s.rightcontext.length > 2 && !stopwords.contains(s.rightcontext[2])) {
            r = new Rule1(RuleType.RIGHT3, s.rightcontext[0], s.rightcontext[1], s.rightcontext[2]);
            addRule(r, s);
         }
      }
      rules = new ArrayList<Rule1>(rulesmap.values());
      rulesmap = null;
   }

   public void addRule(Rule1 r, Sample1 s) {
      Rule1 exists = rulesmap.get(r);
      if (exists == null) {
         rulesmap.put(r, r);
         exists = r;
         r.samples = new HashSet<Sample1>();
      }
      exists.samples.add(s);
   }
   public static int uniqueclusterid = 0;

   public class Cluster extends ArrayList<Rule1> implements Comparable<Cluster> {

      protected HashSet<Sample1> samples;
      public int id = uniqueclusterid++;
      public bow bagofwords;
      protected double innersim = -1;
      protected double inneroverlap = -1;
      protected double meaningful = -1;
      protected double uniqueness = -2;

      private Cluster() {
      }

      public Cluster(Rule1 r) {
         add(r);
      }

      public Cluster clone() {
         Cluster n = new Cluster();
         n.addAll(this);
         n.samples = (HashSet<Sample1>) getSamples().clone();
         n.innersim = innersim;
         n.inneroverlap = inneroverlap;
         n.bagofwords = getBow();
         return n;
      }

      public int hashCode() {
         int hashcode = 31;
         hashcode = MathTools.combineHash(hashcode, id);
         return MathTools.finishHash(hashcode);
      }

      public HashSet<Sample1> getSamples() {
         if (samples == null) {
            samples = new HashSet<Sample1>();
            for (Rule1 r : this) {
               samples.addAll(r.getSamples());
            }
         }
         return samples;
      }

      public bow getBow() {
         if (bagofwords == null) {
            bagofwords = new bow(getSamples());
         }
         return bagofwords;
      }

      public String toString() {
         StringBuilder sb = new StringBuilder();
         for (Rule1 t : this) {
            sb.append(t.toString()).append("\n ");
         }
         return sb.toString();
      }

      public void resetsim() {
         innersim = -1;
         meaningful = -1;
         uniqueness = -2;
      }

      public Cluster merge(Cluster b) {
         Cluster n = new Cluster();
         n.addAll(this);
         n.addAll(b);
         n.samples = (HashSet<Sample1>) getSamples().clone();
         n.samples.addAll(b.getSamples());
         n.bagofwords = null;
         return n;
      }

      public double innersim() {
         if (innersim < 0) {
            
            if (getSamples().size() == 1) {
               innersim = 0;
            } else {
               bow b = getBow();
               double sumcos = 0;
               for (Sample1 s : getSamples()) {
                  bow d = b.minus(s);
                  sumcos += d.cossim(s.getBow());
               }
               innersim = sumcos / getSamples().size();;
            }
         }
         return innersim;
      }

      public double meaningful() {
         double corpusfrequency = repository.getCorpusTF();
         if (meaningful < 0) {
            meaningful = 0;
            for (int i = getBow().termid.length - 1; i >= 0; i--) {
               double exceeds = bagofwords.tfidf[i] - terms.get(bagofwords.termid[i]).cf / corpusfrequency;
               if (exceeds > 0)
                  meaningful += exceeds  ;
            }
         }
         log.info("%f %s", meaningful, this.get(0));
         return meaningful;
      }

       
      public boolean overlaps(Cluster b) {
         if (getSamples().size() < b.getSamples().size()) {
            return b.overlaps(this);
         }
         for (Sample1 s : b.getSamples()) {
            if (getSamples().contains(s)) {
               return true;
            }
         }
         return false;
      }

      public boolean contains(Cluster b) {
         return getSamples().size() >= b.getSamples().size() && getSamples().containsAll(b.getSamples());
      }

      public double overlap(Collection<Sample1> covered) {
         HashSet<Sample1> shared = (HashSet<Sample1>) getSamples().clone();
         shared.retainAll(covered);
         return shared.size() / (double) getSamples().size();
      }

      public ArrayList<Cluster> overlaps() {
         ArrayList<Cluster> list = new ArrayList<Cluster>();
         for (Cluster c : clusters) {
            if (overlaps(c)) {
               list.add(c);
            }
         }
         return list;
      }

      @Override
      public boolean remove(Object o) {
         super.remove(o);
         Rule1 r = (Rule1) o;
         HashSet<Sample1> samples = (HashSet<Sample1>) this.getSamples().clone();
         for (Rule1 rr : this) {
            samples.removeAll(rr.getSamples());
         }
         for (Sample1 s : samples) {
            bagofwords = bagofwords.minus(s);
         }
         this.getSamples().removeAll(samples);
         this.resetsim();
         return true;
      }

      public int compareTo(Cluster o) {
         return (getSamples().size() > o.getSamples().size()) ? -1 : 1;
      }

      @Override
      public boolean add(Rule1 r) {
         this.resetsim();
         return super.add(r);
      }

      @Override
      public boolean removeAll(Collection<?> r) {
         if (r.size() > 0) {
            this.resetsim();
            return super.removeAll(r);
         }
         return false;
      }
   }

   class Winner extends Cluster {

      public Winner(Cluster c) {
         addAll(c);
         this.samples = (HashSet<Sample1>) c.getSamples().clone();
         this.innersim = c.innersim;
         this.bagofwords = c.getBow();
      }

      public Winner(Cluster c, Cluster d) {
         addAll(c);
         this.samples = (HashSet<Sample1>) c.getSamples().clone();
         for (Sample1 s : d.getSamples())
            samples.add(s);
      }

      @Override
      public Winner merge(Cluster b) {
         addAll(b);
         getSamples().addAll(b.getSamples());
         bagofwords = null;
         this.resetsim();
         return this;
      }

      @Override
      public int compareTo(Cluster o) {
         return (getSamples().size() * innersim() > o.getSamples().size() * o.innersim()) ? -1 : 1;
      }
   }

   public class Rule1 extends Rule implements Comparable<Rule1> {

//      double cosa, cosb;
      private HashSet<Sample1> samples;
      public bow bow;
      public double innersim = -1;

      public Rule1(RuleType type, int term1, int term2, int term3) {
         super(-1, term.termid, type, term1, term2, term3);
      }

      public Rule1(RuleType type, int term1, int term2) {
         super(-1, term.termid, type, term1, term2, -1);
      }

      public Rule1(RuleType type, int term1) {
         super(-1, term.termid, type, term1, -1, -1);
      }

      public void resetSamples() {
         samples = null;
      }

      public HashSet<Sample1> getSamples() {
         return samples;
      }

      public bow getBow() {
         if (bow == null) {
            bow = new bow(getSamples());
         }
         return bow;
      }
      
      @Override
      public String toString() {
         StringBuilder sb = new StringBuilder();
         sb.append(type.toString()).append(" ").append(terms.get(word1).term);
         if (terms.containsKey(word2)) {
            sb.append(" ").append(terms.get(word2).term);
         }
         if (terms.containsKey(word3)) {
            sb.append(" ").append(terms.get(word3).term);
         }
         sb.append(" f(").append(cf).append(")");
         return sb.toString();
      }

      public double confidence() {
         switch (type) {
            case LEFT3:
            case RIGHT3:
            case LR:
               return 3;
            case LEFT2:
            case RIGHT2:
               return 2;
            default:
               return 1;
         }
      }

      public int compareTo(Rule1 o) {
         return (samples.size() > o.samples.size()) ? -1 : 1;
      }
   }
}
