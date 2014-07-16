package AOI.Analyze;

import edu.emory.mathcs.backport.java.util.Arrays;
import io.github.repir.Repository.AOI;
import io.github.repir.Repository.AOI.Rule;
import io.github.repir.Repository.AOI.RuleType;
import io.github.repir.Repository.Repository;
import io.github.repir.Repository.Term;
import io.github.repir.Repository.TermContext;
import io.github.repir.Repository.TermContext.Doc;
import io.github.repir.Repository.TermDF;
import io.github.repir.Repository.TermString;
import io.github.repir.tools.DataTypes.TopK;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Lib.MathTools;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * This feature caches the context of all occurrences of a term in the
 * collection. When cached, this feature can be used by other features to
 * analyze the local context a term appears in.
 * <p/>
 * @author jeroen
 */
public class AOIAna {

   public static Log log = new Log(AOIAna.class);
   public int uniqueclusterid = 0;
   public long contextsize;
   public Term term;
   public HashSet<Integer> stopwords;
   public SampleBOW samplesbow[];
   public TermList terms = new TermList();
   Repository repository;
   public ArrayList<Cluster> clusters = new ArrayList<Cluster>();
   public HashMap<RuleSample, RuleSample> rulesmap = new HashMap<RuleSample, RuleSample>();
   public TreeSet<Cluster> winners = new TreeSet<Cluster>();
   public final int width;
   double MININNERSIM = 0.37;
   int MINSAMPLESRULE;
   double MAXOVERLAP;
   double usefulnessthreshold = 0;
   double uniquethreshold = 0.1;
   int MAXSENSE = 64;

   public AOIAna(Repository repository, Term term) {
      this.repository = repository;
      stopwords = repository.getStopwords();
      this.term = term;
      width = repository.configuredInt("aoi.width", 10);
      MININNERSIM = repository.configuredDouble("aoi.sim", 0.35);
      MAXOVERLAP = 1 - repository.configuredDouble("aoi.novel", 0.10);
   }

   public ArrayList<Rule> process() {
      log.startTime();
      ArrayList<Rule> result = new ArrayList<Rule>();
      int sense = 0;
      Sample samples[] = readSamples();
      log.reportTime("Data read samples=%d", samples.length);
      buildTermList(samples);
      log.reportTime("Term list built");
      RuleSample rulesarray[] = generateNewRules(samples);
      samples = null;
      //terms = null;
      log.reportTime("rules generated %d", rulesarray.length);

      log.reportTime("sorted");

      TopK<Cluster> topk = new TopK<Cluster>(MAXSENSE * 8, wincomparator);
      for (int r = 0; r < rulesarray.length; r++) {
         Cluster current = new Cluster(rulesarray[r]);
         //double overlap = current.overlap(covered);
         if (current.innersim() > MININNERSIM) {
            topk.add(current);
         }
         rulesarray[r] = null;
      }
      rulesarray = null;
      sense = 0;
      winners = new TreeSet<Cluster>(topk);
      pruneWinners();
      log.reportTime("winners pruned");
      HashSet<Integer> covered = new HashSet<Integer>();
      for (Cluster w : winners) {
         for (RuleSample r : w) {
            r.cf = w.getSamples().size();
            HashSet<Integer> docids = new HashSet<Integer>();
            for (Integer s : w.getSamples()) {
               docids.add(samplesbow[s].docid);
            }
            r.df = docids.size();
            if (sense < MAXSENSE) {
               result.add(r.setSense(sense));
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
      Rule r = new Rule(MAXSENSE, term.getID(), RuleType.NORULE, 0);
      r.cf = samplesbow.length - covered.size();
      result.add(r);
      return result;
   }

   public void pruneWinners() {
      HashSet<Integer> covered = new HashSet<Integer>();
      Iterator<Cluster> iter = winners.iterator();
      while (iter.hasNext()) {
         Cluster w = iter.next();
         if (w.innersim() < MININNERSIM) {
            iter.remove();
         } else if (w.overlap(covered) >= MAXOVERLAP) {
            iter.remove();
         } else {
            covered.addAll(w.getSamples());
         }
      }
   }

   public Sample[] readSamples() {
      ArrayList<Sample> samples = new ArrayList<Sample>();
      TermContext termcontext = (TermContext) repository.getFeature(TermContext.class, term.getProcessedTerm());
      java.util.HashMap<Doc, ArrayList<TermContext.Sample>> docsamples = termcontext.readSamples();
      for (Map.Entry<Doc, ArrayList<TermContext.Sample>> e : docsamples.entrySet()) {
         for (TermContext.Sample s : e.getValue()) {
            samples.add(new Sample(e.getKey().docid, s.leftcontext, s.rightcontext));
         }
         if (samples.size() > 1000000)
            break;
      }
      repository.unloadStoredDynamicFeature(termcontext);
      return samples.toArray(new Sample[samples.size()]);
   }

   public void buildTermList(Sample samples[]) {
      TermDF df = (TermDF) repository.getFeature(TermDF.class);
      contextsize = 0;
      terms.add(term.getID());
      for (Sample sample : samples) {
         contextsize += sample.leftcontext.length + sample.rightcontext.length;
         for (int i : sample.leftcontext) {
            if (i != term.getID()) {
               TermStats t = terms.getOrAdd(i);
               //t.samples.add(sample);
            }
         }
         for (int i : sample.rightcontext) {
            if (i != term.getID()) {
               TermStats t = terms.getOrAdd(i);
               //t.samples.add(sample);
            }
         }
      }
      TermString termstring = (TermString) repository.getFeature(TermString.class);
      for (TermStats t : terms.values()) {
         t.term = termstring.readValue(t.termid);
      }
      df.loadMem();
      for (TermStats t : terms.values()) {
         t.setDF(df.readValue(t.termid));
      }
      df.unloadMem();
      df = null;
   }

   public bow bagOfWords(Collection<Integer> set) {
      HashMap<Integer, Double> bow = new HashMap<Integer, Double>();
      for (Integer si : set) {
         bow b = samplesbow[si].getBow();
         for (int i = b.termid.length - 1; i >= 0; i--) {
            Double c = bow.get(b.termid[i]);
            if (c == null) {
               bow.put(b.termid[i], b.tfidf[i]);
            } else {
               bow.put(b.termid[i], c + b.tfidf[i]);
            }

         }
      }
      return new bow(bow);
   }

   public HashMap<Integer, Double> bagOfWordsS(Collection<Sample> set) {
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
      for (Map.Entry<Integer, Double> e : bow.entrySet()) {
         TermStats s = terms.get(e.getKey());
         bow.put(e.getKey(), e.getValue() * s.idf);
      }
      return bow;
   }

   class TermStats {

      double idf;
      int termid;
      String term;

      public TermStats(int termid) {
         this.termid = termid;
      }

      public int hashCode() {
         return termid;
      }

      public boolean equals(Object o) {
         return (((TermStats) o).termid == termid);
      }

      public void setDF(long df) {
         idf = Math.log(repository.getDocumentCount() / (double) df);
      }
   }

   class TermList extends HashMap<Integer, TermStats> {

      public void add(int termid) {
         put(termid, new TermStats(termid));
      }

      public TermStats getOrAdd(int termid) {
         TermStats t = get(termid);
         if (t == null) {
            t = new TermStats(termid);
            put(termid, t);
         }
         return t;
      }
   }

   public RuleSample[] generateNewRules(Sample samples[]) {
      RuleSample r;

      samplesbow = new SampleBOW[samples.length];
      //word to left
      for (int s = 0; s < samples.length; s++) {
         if (s % 1000 == 0) {
            log.info("generateNewRules %d", s);
         }
         Sample sample = samples[s];
         samplesbow[s] = new SampleBOW(sample);
         if (sample.leftcontext.length > 0 && !stopwords.contains(sample.leftcontext[0])) {
            r = new RuleSample(AOI.RuleType.LEFT, sample.leftcontext[0]);
            addRule(r, s);
         }

         // word to right
         if (sample.rightcontext.length > 0 && !stopwords.contains(sample.rightcontext[0])) {
            r = new RuleSample(AOI.RuleType.RIGHT, sample.rightcontext[0]);
            addRule(r, s);
         }

         // word in window
         for (int p = Math.min(width, sample.leftcontext.length) - 1; p >= 0; p--) {
            int termid = sample.leftcontext[p];
            if (!stopwords.contains(termid) && termid != term.getID()) {
               r = new RuleSample(AOI.RuleType.WINDOW, termid);
               addRule(r, s);
            }
         }
         for (int p = Math.min(width, sample.rightcontext.length) - 1; p >= 0; p--) {
            int termid = sample.rightcontext[p];
            if (!stopwords.contains(termid) && termid != term.getID()) {
               r = new RuleSample(AOI.RuleType.WINDOW, termid);
               addRule(r, s);
            }
         }

         // word to LR
         if (sample.leftcontext.length > 0 && sample.rightcontext.length > 0 && !stopwords.contains(sample.rightcontext[0])) {
            r = new RuleSample(AOI.RuleType.LR, sample.leftcontext[0], sample.rightcontext[0]);
            addRule(r, s);
         }

         // 2 word WINDOW
         for (int p = Math.min(width, sample.leftcontext.length) - 2; p >= 0; p--) {
            if (!stopwords.contains(sample.leftcontext[p]) && sample.leftcontext[p] != term.getID() && sample.leftcontext[p + 1] != term.getID()) {
               r = new RuleSample(AOI.RuleType.WINDOW2, sample.leftcontext[p + 1], sample.leftcontext[p]);
               addRule(r, s);
            }
         }
         for (int p = Math.min(width, sample.rightcontext.length) - 2; p >= 0; p--) {
            if (!stopwords.contains(sample.rightcontext[p + 1]) && sample.rightcontext[p] != term.getID() && sample.rightcontext[p + 1] != term.getID()) {
               r = new RuleSample(AOI.RuleType.WINDOW2, sample.rightcontext[p], sample.rightcontext[p + 1]);
               addRule(r, s);
            }
         }

         // 2 word to RIGHT
         if (sample.rightcontext.length > 1 && !stopwords.contains(sample.rightcontext[1])) {
            r = new RuleSample(AOI.RuleType.RIGHT2, sample.rightcontext[0], sample.rightcontext[1]);
            addRule(r, s);
         }

         // 2 word to LEFT
         if (sample.leftcontext.length > 1 && !stopwords.contains(sample.leftcontext[1])) {
            r = new RuleSample(AOI.RuleType.LEFT2, sample.leftcontext[1], sample.leftcontext[0]);
            addRule(r, s);
         }

//         if (sample.leftcontext.length > 2 && !stopwords.contains(sample.leftcontext[2])) {
//            r = new RuleSample(AOI.RuleType.LEFT3, sample.leftcontext[2], sample.leftcontext[1], sample.leftcontext[0]);
//            addRule(r, s);
//         }
//         if (sample.rightcontext.length > 2 && !stopwords.contains(sample.rightcontext[2])) {
//            r = new RuleSample(AOI.RuleType.RIGHT3, sample.rightcontext[0], sample.rightcontext[1], sample.rightcontext[2]);
//            addRule(r, s);
//         }
         samples[s] = null;
      }
      MINSAMPLESRULE = (int) Math.pow(Math.log10(samples.length), 2);
      ArrayList<RuleSample> list = new ArrayList<RuleSample>();
      for (RuleSample rr : rulesmap.values()) {
         if (rr.getSamples().size() >= MINSAMPLESRULE) {
            list.add(rr);
         }
      }
      rulesmap = null;
      Collections.sort(list);
      return list.toArray(new RuleSample[list.size()]);
   }

   public void addRule(RuleSample r, Integer sample) {
      RuleSample exists = rulesmap.get(r);
      if (exists == null) {
         rulesmap.put(r, r);
         exists = r;
      }
      exists.addSample(sample);
   }

   class bow {

      int termid[];
      double tfidf[];
      double magnitude = 0;
      int size = -1;

      public bow() {
         termid = new int[0];
         tfidf = new double[0];
      }

      public bow(Collection<Sample> set) {
         this(bagOfWordsS(set));
      }

      public bow(HashMap<Integer, Double> keys) {
         TreeMap<Integer, Double> sorted = new TreeMap<Integer, Double>(keys);
         termid = new int[keys.size()];
         tfidf = new double[keys.size()];
         Iterator<Map.Entry<Integer, Double>> iter = sorted.entrySet().iterator();
         for (int i = 0; i < keys.size(); i++) {
            Map.Entry<Integer, Double> e = iter.next();
            termid[i] = e.getKey();
            tfidf[i] = e.getValue();
         }
      }

      public bow clone() {
         bow b = new bow();
         b.termid = new int[termid.length];
         b.tfidf = new double[termid.length];
         System.arraycopy(termid, 0, b.termid, 0, termid.length);
         System.arraycopy(tfidf, 0, b.tfidf, 0, termid.length);
         b.magnitude = magnitude;
         return b;
      }

      public void merge(bow b) {
         int i = 0;
         int add = 0;
         for (int key : b.termid) {
            for (; i < termid.length && termid[i] < key; i++) {
               add++;
            }
            if (i == termid.length || termid[i] != key) {
               add++;
            }
         }
         add += (termid.length - i);
         int newtermid[] = new int[add];
         double newfreq[] = new double[add];
         i = 0;
         int newi = 0;
         for (int j = 0; j < b.termid.length;) {
            for (; i < termid.length && termid[i] < b.termid[j];) {
               newtermid[newi] = termid[i];
               newfreq[newi++] = tfidf[i++];
            }
            if (i == termid.length || termid[i] != b.termid[j]) {
               newtermid[newi] = b.termid[j];
               newfreq[newi++] = b.tfidf[j++];
            } else {
               newtermid[newi] = b.termid[j];
               newfreq[newi++] = b.tfidf[j++] + tfidf[i++];
            }
         }
         for (; i < termid.length;) {
            newtermid[newi] = termid[i];
            newfreq[newi++] = tfidf[i++];
         }
         termid = newtermid;
         tfidf = newfreq;
         magnitude = 0;
      }

      public double magnitude() {
         if (magnitude == 0) {
            long sum = 0;
            for (int i = 0; i < termid.length; i++) {
               if (termid[i] != term.getID()) {
                  sum += ((long) tfidf[i] * (long) tfidf[i]);
               }
            }
            magnitude = Math.sqrt(sum);
         }
         return magnitude;
      }

      public double size() {
         if (size < 0) {
            size = 0;
            for (int i = 0; i < termid.length; i++) {
               if (termid[i] != term.getID()) {
                  size += tfidf[i];
               }
            }
         }
         return size;
      }

      public double avgOverlap(bow b) {
         return overlap(b) / Math.min(size(), b.size());
      }

      public double cossim(bow b) {
         double m = (b.magnitude() * magnitude());
         if (m == 0) {
            return 0;
         }
         return dotProduct(b) / m;
      }

      public long dotProduct(bow b) {
         if (termid.length > b.termid.length) {
            return b.dotProduct(this);
         }
         long sum = 0;
         int j = 0;
         LOOP:
         for (int i = 0; i < termid.length; i++) {
            for (; b.termid[j] < termid[i];) {
               if (++j == b.termid.length) {
                  break LOOP;
               }
            }
            if (b.termid[j] == termid[i] && termid[i] != term.getID()) {
               sum += tfidf[i] * b.tfidf[j];
            }
         }
         return sum;
      }

      public double overlap(bow b) {
         if (termid.length > b.termid.length) {
            return b.overlap(this);
         }
         double fac = size() / b.size;
         int sum = 0;
         int j = 0;
         LOOP:
         for (int i = 0; i < termid.length; i++) {
            for (; b.termid[j] < termid[i];) {
               if (++j == b.termid.length) {
                  break LOOP;
               }
            }
            if (b.termid[j] == termid[i] && termid[i] != term.getID()) {
               sum += Math.min(tfidf[i], fac * b.tfidf[j]);
            }
         }
         return sum / size();
      }

      public bow minus(bow o) {
         bow b = clone();
         for (int i = o.termid.length - 1; i >= 0; i--) {
            int pos = Arrays.binarySearch(termid, o.termid[i]);
            b.tfidf[pos] -= o.tfidf[i];
         }
         b.magnitude = 0;
         return b;
      }

      public String toString() {
         StringBuilder sb = new StringBuilder();
         for (int i = 0; i < termid.length; i++) {
            if (stopwords.contains(termid[i])) {
               sb.append("[").append(termid[i]).append("=>").append(tfidf[i]).append("] ");
            } else {
               sb.append(termid[i]).append("=>").append(tfidf[i]).append(" ");
            }
         }
         return sb.toString();
      }
   }

   class Sample {

      public int leftcontext[];
      public int rightcontext[];
      public int docid;

      public Sample(int docid, int leftcontext[], int rightcontext[]) {
         this.leftcontext = leftcontext;
         this.rightcontext = rightcontext;
         this.docid = docid;
      }

      public String toString() {
         StringBuilder sb = new StringBuilder();
         for (int i = leftcontext.length - 1; i >= 0; i--) {
            sb.append(terms.get(leftcontext[i]).term).append(" ");
         }
         sb.append("* ");
         for (int i : rightcontext) {
            sb.append(terms.get(i).term).append(" ");
         }
         return sb.toString();
      }
   }

   class SampleBOW {

      bow bow;
      int docid;

      public SampleBOW(final Sample s) {
         this.docid = s.docid;
         bow = new bow(new ArrayList<Sample>() {
            {
               add(s);
            }
         });
      }

      public bow getBow() {
         return bow;
      }
   }

   public class RuleSample extends AOI.Rule implements Comparable<RuleSample> {

//      double cosa, cosb;
      private HashSet<Integer> samples = new HashSet<Integer>();
      private bow bow;
      public double innersim = -1;

      public RuleSample(AOI.RuleType type, int term1, int term2) {
         super(-1, term.getID(), type, term1, term2);
      }

      public RuleSample(AOI.RuleType type, int term1) {
         super(-1, term.getID(), type, term1);
      }

      public HashSet<Integer> getSamples() {
         return samples;
      }

      public void addSample(int sample) {
         samples.add(sample);
      }

      public bow getBow() {
         if (bow == null) {
            bow = bagOfWords(samples);
         }
         return bow;
      }

      @Override
      public String toString() {
         StringBuilder sb = new StringBuilder();
         sb.append(type.toString());
         if (terms != null) {
            sb.append(" ").append(terms.get(word0).term);
            if (word1 >= 0) {
               sb.append(" ").append(terms.get(word1).term);
            }
         } else {
            sb.append(" ").append(word0);
            if (word1 >= 0) {
               sb.append(" ").append(word1);
            }
         }
         sb.append(" f(").append(cf).append(")");
         return sb.toString();
      }

      public int compareTo(RuleSample o) {
         return (samples.size() > o.samples.size()) ? -1 : 1;
      }
   }

   public Comparator<Cluster> wincomparator = new Comparator<Cluster>() {
      @Override
      public int compare(Cluster o1, Cluster o2) {
         return (o1.score() > o2.score()) ? 1 : -1;
      }
   };

   public class Cluster extends ArrayList<RuleSample> implements Comparable<Cluster> {

      protected HashSet<Integer> samples;
      public int id = uniqueclusterid++;
      public bow bagofwords;
      protected double innersim = -1;
      protected double inneroverlap = -1;
      protected double usefulness = -2;
      protected double uniqueness = -2;
      final ArrayList<Cluster> subclusters = new ArrayList<Cluster>();

      public Cluster(Cluster... c) {
         addAll(c[0]);
         subclusters.addAll(c[0].subclusters);
         samples = (HashSet<Integer>) c[0].samples.clone();
         bagofwords = c[0].bagofwords.clone();
         for (int i = 1; i < c.length; i++) {
            Cluster w = c[i];
            addAll(w);
            subclusters.add(w);
            for (Integer s : w.getSamples()) {
               if (!samples.contains(s)) {
                  samples.add(s);
                  bagofwords.merge(samplesbow[s].bow);
               }
            }
         }
      }

      private Cluster() {
      }

      public Cluster(RuleSample r) {
         add(r);
         samples = (HashSet<Integer>) r.samples.clone();
         bagofwords = r.getBow().clone();
      }

      public Cluster merge(Cluster b) {
         if (subclusters.size() == 0) {
            return new Cluster(this, b);
         }
         ArrayList<Cluster> clusters = (ArrayList<Cluster>) subclusters.clone();
         clusters.add(b);
         return new Cluster(clusters.toArray(new Cluster[clusters.size()]));
      }

      public double score() {
         return Math.sqrt(getSamples().size()) * innersim();
      }

      @Override
      public int compareTo(Cluster o) {
         return (score() > o.score()) ? -1 : 1;
      }

      public Cluster clone() {
         Cluster n = new Cluster();
         n.addAll(this);
         n.samples = (HashSet<Integer>) getSamples().clone();
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

      public HashSet<Integer> getSamples() {
         return samples;
      }

      public bow getBow() {
         return bagofwords;
      }

      public String toString() {
         StringBuilder sb = new StringBuilder();
         for (RuleSample t : this) {
            sb.append(t.toString()).append("\n ");
         }
         return sb.toString();
      }

      public void resetsim() {
         innersim = -1;
         usefulness = -2;
         uniqueness = -2;
      }

      public double innersim() {
         if (innersim < 0) {
            if (getSamples().size() == 1) {
               innersim = 0;
            } else {
               bow b = getBow();
               double sumcos = 0;
               for (Integer s : getSamples()) {
                  bow d = b.minus(samplesbow[s].getBow());
                  sumcos += d.cossim(samplesbow[s].getBow());
               }
               innersim = sumcos / getSamples().size();;
            }
         }
         return innersim;
      }

      public double inneroverlap() {
         if (inneroverlap < 0) {
            if (getSamples().size() == 1) {
               inneroverlap = 0;
            } else {
               bow b = getBow();
               double sumoverlap = 0;
               double size = 0;
               for (Integer s : getSamples()) {
                  bow d = b.minus(samplesbow[s].getBow());
                  sumoverlap += samplesbow[s].getBow().overlap(d);
                  size += Math.min(d.size(), samplesbow[s].getBow().size());
               }
               inneroverlap = sumoverlap / size;
            }
         }
         return inneroverlap;
      }

//      public double uniqueness(Collection<Sample> nonunique) {
//         if (uniqueness < 0) {
//            double sumdotp = 0;
//            double summag = 0;
//            HashSet<Sample> unique = (HashSet<Sample>) getSamples().clone();
//            unique.removeAll(nonunique);
//            if (unique.size() < MINSAMPLESIZE) {
//               return 0;
//            }
//            HashSet<Sample> shared = (HashSet<Sample>) getSamples().clone();
//            shared.removeAll(unique);
//            if (shared.size() == 0) {
//               return innersim();
//            }
//            for (Sample s : unique) {
//               for (Sample r : shared) {
//                  sumdotp += s.getBow().dotProduct(r.getBow());
//                  summag += s.getBow().magnitude() * r.getBow().magnitude();
//               }
//            }
//            //log.info("uniqueness %f unique %d shared %d nonunique %d cluster %s", sum / (unique.size() * shared.size()), unique.size(), shared.size(), nonunique.size(), this);
//            uniqueness = sumdotp / summag;
//         }
//         return uniqueness;
//      }
      public double cossim(Cluster o) {
         HashSet<Integer> sa = (HashSet<Integer>) getSamples().clone();
         sa.addAll(o.getSamples());
         bow b = this.getBow().clone();
         b.merge(o.getBow());
         double sumcos = 0;
         for (Integer s : sa) {
            bow d = b.minus(samplesbow[s].getBow());
            sumcos += d.cossim(samplesbow[s].getBow());
         }
         return sumcos / getSamples().size();
      }

      public double csscore(Cluster b) {
         return innersim() * Math.sqrt(getSamples().size());
      }

      public boolean overlaps(Cluster b) {
         if (getSamples().size() < b.getSamples().size()) {
            return b.overlaps(this);
         }
         for (Integer s : b.getSamples()) {
            if (getSamples().contains(s)) {
               return true;
            }
         }
         return false;
      }

      public boolean contains(Cluster b) {
         return getSamples().size() >= b.getSamples().size() && getSamples().containsAll(b.getSamples());
      }

      public double overlap(Collection<Integer> covered) {
         HashSet<Integer> shared = (HashSet<Integer>) getSamples().clone();
         shared.retainAll(covered);
         return shared.size() / (double) getSamples().size();
      }

      @Override
      public boolean add(RuleSample r) {
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

}
