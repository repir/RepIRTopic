package io.github.repir.Strategy;

import io.github.repir.Strategy.Analyze;
import io.github.repir.Strategy.Term;
import io.github.repir.Strategy.Tools.StopWords;
import io.github.repir.Strategy.Collector.AOICollector;
import io.github.repir.Repository.TermContext;
import edu.emory.mathcs.backport.java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import io.github.repir.Repository.StopwordsCache;
import io.github.repir.Repository.TermString;
import io.github.repir.Repository.TermTF;
import io.github.repir.Retriever.Retriever;
import io.github.repir.Repository.TermContext.Doc;
import io.github.repir.Repository.TermContext.Sample;
import io.github.repir.Repository.TermDF;
import io.github.repir.tools.DataTypes.HashMap;
import io.github.repir.tools.Lib.Log;

/**
 * This feature caches the context of all occurrences of a term in the
 * collection. When cached, this feature can be used by other features to
 * analyze the local context a term appears in.
 * <p/>
 * @author jeroen
 */
public abstract class AOIAnalyzer extends Analyze {

   public static Log log = new Log(AOIAnalyzer.class);
   public long contextsize;
   public TermContext termcontext;
   public Term term;
   public AOICollector collector;
   public HashSet<Integer> stopwords;
   public ArrayList<Sample1> samples = new ArrayList<Sample1>();
   public TermList terms = new TermList();
   TermTF tf = (TermTF) repository.getFeature("TermTF");
   TermDF df = (TermDF) repository.getFeature("TermDF");
   TermString termstring = (TermString) repository.getFeature("TermString");
   StopWords sw;

   public AOIAnalyzer(Retriever retriever) {
      super(retriever);
      
      StopwordsCache swc = (StopwordsCache)repository.getFeature("StopwordsCache");
      stopwords = swc.getStopwords();
   }

   public void readCachedData() {
      termcontext = (TermContext) repository.getFeature("TermContext");
      java.util.HashMap<Doc, ArrayList<Sample>> docsamples = termcontext.read(term.termid);
      for (Map.Entry<Doc, ArrayList<Sample>> e : docsamples.entrySet()) {
         for (Sample s : e.getValue()) {
            samples.add(new Sample1(s.pos, e.getKey().docid, e.getKey().partition, s.leftcontext, s.rightcontext));
         }
      }
      buildTermList();
   }

   public Term getTerm() {
      return term;
   }

   @Override
   public void setCollector() {
      if (collector != null) {
         log.crash();
      }
      retriever.tokenizeQuery(query);
      term = new Term(repository, query.stemmedquery.split("\\s+")[0]);
      collector = new AOICollector(this);
   }

   public void buildTermList() {
      contextsize = 0;
      terms.add(term.termid);
      for (Sample1 sample : samples) {
         contextsize += sample.leftcontext.length + sample.rightcontext.length;
         for (int i : sample.leftcontext) {
            if (i != term.termid) {
               TermStats t = terms.getOrAdd(i);
               t.freq++;
               //t.samples.add(sample);
            }
         }
         for (int i : sample.rightcontext) {
            if (i != term.termid) {
               TermStats t = terms.getOrAdd(i);
               t.freq++;
               //t.samples.add(sample);
            }
         }
      }
      tf.loadMem();
      for (TermStats t : terms.values()) {
         t.cf = tf.readValue(t.termid);
         t.term = termstring.readValue(t.termid);
      }
      tf.unloadMem();
      df.loadMem();
      for (TermStats t : terms.values()) {
         t.setDF( df.readValue(t.termid) );
      }
      df.unloadMem();
      tf = null;
      df = null;
      termstring = null;
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
      for (Map.Entry<Integer, Double> e : bow.entrySet()) {
         TermStats s = terms.get(e.getKey());
         bow.put(e.getKey(), e.getValue() * s.idf);
      }
      return bow;
   }

   @Override
   public void prepareAggregationDetail() {
   }

   class TermStats {

      long cf;
      double idf;
      int termid;
      int freq;
      String term;
      //HashSet<Sample1> samples = new HashSet<Sample1>();

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
         idf = Math.log( repository.getDocumentCount() / (double)df );
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

   class bow {

      int termid[];
      double tfidf[];
      double magnitude = 0;
      int size = -1;

      private bow() {
      }

      public bow(Collection<Sample1> set) {
         HashMap<Integer, Double> keys = bagOfWords(set);
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
         if (termid.length < b.termid.length) {
            b.merge(this);
            termid = b.termid;
            tfidf = b.tfidf;
         } else {
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
         }
         magnitude = 0;
      }

      public double magnitude() {
         if (magnitude == 0) {
            long sum = 0;
            for (int i = 0; i < termid.length; i++) {
               if (termid[i] != term.termid) {
                  sum += ((long)tfidf[i] * (long)tfidf[i]);
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
               if (termid[i] != term.termid) {
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
            if (b.termid[j] == termid[i] && termid[i] != term.termid) {
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
            if (b.termid[j] == termid[i] && termid[i] != term.termid) {
               sum += Math.min(tfidf[i], fac * b.tfidf[j]);
            }
         }
         return sum / size();
      }

      public bow minus(Sample s) {
         bow b = clone();
         for (int i : s.leftcontext) {
            if (!stopwords.contains(i)) {
               int pos = Arrays.binarySearch(termid, i);
               b.tfidf[pos]--;
            }
         }
         for (int i : s.rightcontext) {
            if (!stopwords.contains(i)) {
               int pos = Arrays.binarySearch(termid, i);
               b.tfidf[pos]--;
            }
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
   static private int uniqueid = 0;

   class Sample1 extends Sample {

      final int id = uniqueid++;
      public long docid;
      bow bow;

      public Sample1(int pos, int docid, int partition, int leftcontext[], int rightcontext[]) {
         super(pos, leftcontext, rightcontext);
         this.docid = docid | ((long)partition << 32);
      }

      @Override
      public boolean equals(Object o) {
         return ((o instanceof Sample1) && ((Sample1) o).id == id);
      }

      @Override
      public int hashCode() {
         int hash = 5;
         hash = 47 * hash + this.id;
         return hash;
      }

      public bow getBow() {
         if (bow == null) {
            ArrayList<Sample1> l = new ArrayList<Sample1>();
            l.add(this);
            bow = new bow(l);
         }
         return bow;
      }

      public void print() {
         StringBuilder sb = new StringBuilder();
         for (int i = leftcontext.length - 1; i >= 0; i--) {
            sb.append(terms.get(leftcontext[i]).term).append(" ");
         }
         sb.append("* ");
         for (int i : rightcontext) {
            sb.append(terms.get(i).term).append(" ");
         }
         log.printf("%s", sb);
      }
   }
}
