package io.github.repir.Repository;

import io.github.repir.Repository.StoredDynamicFeature;
import io.github.repir.Repository.TermString;
import io.github.repir.Repository.Repository;
import java.util.ArrayList;
import io.github.repir.Repository.AOI.File;
import io.github.repir.Repository.AOI.Record;
import io.github.repir.tools.Content.Datafile;
import io.github.repir.tools.Content.RecordHeaderData;
import io.github.repir.tools.Content.RecordHeaderDataRecord;
import io.github.repir.tools.Content.RecordSortHash;
import io.github.repir.tools.Content.RecordSortHashRecord;
import io.github.repir.tools.Lib.ArrayTools;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Lib.MathTools;
import io.github.repir.tools.Lib.PrintTools;

/**
 * contains the rules to disambiguate between the senses of a word
 *
 * @author jer
 */
public class AOI extends StoredDynamicFeature<File, Record> {

   public static Log log = new Log(AOI.class);

   protected AOI(Repository repository) {
      super(repository);
   }

   public AOI clone() {
      return new AOI(repository);
   }

   @Override
   public File createFile(Datafile datafile) {
      return new File(datafile);
   }

   @Override
   public void setBufferSize(int size) {
      getFile().setBufferSize(size);
   }

   public ArrayList<Rule> read(int termid) {
      ArrayList<Rule> list = new ArrayList<Rule>();
      Record r = getFile().newRecord();
      r.term = termid;
      Record found = getFile().find(r);
      //log.info("found %s", found);
      if (found != null) {
         for (int ri[] : found.rules) {
            Rule rule = new Rule(ri);
            rule.sense = list.size();
            list.add(rule);
         }
      }
      return list;
   }

   public class File extends RecordHeaderData<Record> {

      public CIntField term = this.addCInt("term");
      public CIntArray2Field rules = this.addCIntArray2("rules");

      public File(Datafile df) {
         super(df);
      }

      @Override
      public Record newRecord() {
         return new Record();
      }
   }

   public class Record implements RecordHeaderDataRecord<File> {

      public int term;
      public int rules[][];

      public String toString() {
         return PrintTools.sprintf("bucketindex=%d term=%s", this.hashCode(), term);
      }

      @Override
      public int hashCode() {
         return MathTools.finishHash(MathTools.combineHash(31, term));
      }

      public void dumpvalue() {
         rules = null;
      }

      @Override
      public boolean equals(Object r) {
         if (r instanceof Record) {
            Record record = (Record) r;
            if (term == record.term) {
               return true;
            }
         }
         return false;
      }

      public void write(File file) {
         file.term.write(term);
         file.rules.write(rules);
      }

      public void read(File file) {
            term = file.term.value;
            rules = file.rules.value;
      }

      public void convert(RecordHeaderDataRecord record) {
         Record r = (Record) record;
         r.rules = rules;
         r.term = term;
      }
   }

   public static enum RuleType {

      LEFT,
      RIGHT,
      LEFT2,
      RIGHT2,
      LEFT3,
      RIGHT3,
      LR,
      WINDOW,
      WINDOW2,
      NORULE
   }

   public static class Rule {

      public int sense;
      public int termid;
      public final RuleType type;
      public final int word1;
      public final int word2;
      public final int word3;
      public int cf;
      public int df;

      public Rule(int sense, int termid, RuleType type, int word) {
         this(sense, termid, type, word, 0, 0);
      }

      public Rule(int sense, int termid, RuleType type, int word, int word2) {
         this(sense, termid, type, word, word2, 0);
      }

      public Rule(int sense, int termid, RuleType type, int word, int word2, int word3) {
         this.sense = sense;
         this.termid = termid;
         this.type = type;
         this.word1 = word;
         this.word2 = word2;
         this.word3 = word3;
      }

      public Rule(int a[]) {
         this(a[0], a[1], RuleType.values()[a[2]], a[3], a[4], a[5]);
         cf = a[6];
         df = a[7];
      }

      public Rule setSense(int sense) {
         Rule r = new Rule(sense, termid, type, word1, word2, word3);
         r.cf = cf;
         r.df = df;
         return r;
      }

      public int[] toIntArray() {
         int a[] = new int[8];
         a[0] = sense;
         a[1] = termid;
         a[2] = type.ordinal();
         a[3] = word1;
         a[4] = word2;
         a[5] = word3;
         a[6] = cf;
         a[7] = df;
         return a;
      }

      public boolean match(int left[], int right[]) {
         switch (type) {
            case LEFT:
               return left.length > 0 && left[0] == word1;
            case RIGHT:
               return right.length > 0 && right[0] == word1;
            case LEFT2:
               return left.length > 1 && left[1] == word1 && left[0] == word2;
            case RIGHT2:
               return right.length > 1 && right[0] == word1 && right[1] == word2;
            case LEFT3:
               return left.length > 2 && left[2] == word1 && left[1] == word2 && left[0] == word3;
            case RIGHT3:
               return right.length > 2 && right[0] == word1 && right[1] == word2 && right[2] == word3;
            case LR:
               return left.length > 0 && right.length > 0 && left[0] == word1 && right[0] == word2;
            case WINDOW:
               return ArrayTools.contains(word1, left) || ArrayTools.contains(word1, right);
            case WINDOW2:
               for (int i = left.length - 2; i >= 0; i--) {
                  if (left[i] == word2 && left[i + 1] == word1) {
                     return true;
                  }
               }
               for (int i = right.length - 2; i >= 0; i--) {
                  if (right[i] == word1 && right[i + 1] == word2) {
                     return true;
                  }
               }
               if (word1 == termid && right.length > 0 && right[0] == word2) {
                  return true;
               }
               if (word2 == termid && left.length > 0 && left[0] == word1) {
                  return true;
               }
               return false;
            default:
               return false;
         }
      }

      public String toString(Repository r) {
         StringBuilder sb = new StringBuilder();
         TermString ts = (TermString) r.getFeature("TermString");
         sb.append(sense).append(" ").append(type).append(" ").append(ts.readValue(word1)).append(" ");
         if (word2 >= 0) {
            sb.append(ts.readValue(word2)).append(" ");
         }
         if (word3 >= 0) {
            sb.append(ts.readValue(word3));
         }
         sb.append(" cf(").append(this.cf).append(")");
         sb.append(" df(").append(this.df).append(")");
         return sb.toString();
      }

      public boolean equals(Object o) {
         Rule r = (Rule) o;
         return (r.sense == sense && r.type == type && r.word1 == word1 && r.word2 == word2 && r.word3 == word3);
      }

      public int hashCode() {
         return MathTools.finishHash(MathTools.combineHash(31, type.ordinal(), word1, word2, word3));
      }
   }

   public static class RuleSet extends ArrayList<Rule> {

      int termid;
      public final int width;

      public RuleSet(Repository repository, int termid) {
         width = repository.getConfigurationInt("aoi.width", 10);
         this.termid = termid;
         AOI termsense = (AOI) repository.getFeature("AOI");
         termsense.openRead();
         addAll(termsense.read(termid));
      }

      public long matchOccurrence(int left[], int right[]) {
         long areas = 0;
         for (Rule r : this) {
            if (r.type != RuleType.NORULE && r.match(left, right)) {
               areas |= (1l << r.sense);
               log.info("match %d %s", r.sense, Long.toBinaryString(areas));
            }
         }
               log.info("match %s", Long.toBinaryString(areas));
         return areas;
      }

      public long[] matchAll(int content[]) {
         return matchAll(content, getTermPos(content));
      }

      public long[] matchAll(int content[], int pos[]) {
         long sense[] = new long[pos.length];
         for (int i = 0; i < pos.length; i++) {
            int p = pos[i];
            int startleft = (p < width) ? 0 : p - width;
            int endright = (p + width + 1 < content.length) ? p + width + 1: content.length;
            int left[] = new int[p - startleft];
            int right[] = new int[endright - (p + 1)];
            for (int j = 0; j < left.length; j++) {
               left[j] = content[ p - j - 1];
            }
            System.arraycopy(content, p + 1, right, 0, right.length);
            log.info("pos %d", pos[i]);
            log.info("left %s", ArrayTools.toString(left));
            log.info("right %s", ArrayTools.toString(right));
            sense[i] = matchOccurrence(left, right);
            for (int b = 0; b < 64; b++) {
               if (((1l << b) & sense[i]) != 0) 
                  log.printf("sense %d", b);
            }
         }
         return sense;
      }

      public int[] getTermPos(int content[]) {
         ArrayList<Integer> pos = new ArrayList<Integer>();
         for (int p = 0; p < content.length; p++) {
            if (content[p] == termid) {
               pos.add(p);
            }
         }
         return ArrayTools.toIntArray(pos);
      }
   }
}
