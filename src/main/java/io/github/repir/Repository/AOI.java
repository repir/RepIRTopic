package io.github.repir.Repository;

import io.github.repir.Repository.AOI.File;
import io.github.repir.Repository.AOI.Record;
import io.github.repir.tools.Content.Datafile;
import io.github.repir.tools.Lib.ArrayTools;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Lib.MathTools;
import io.github.repir.tools.Lib.PrintTools;
import io.github.repir.tools.Structure.StructuredFileKeyValue;
import io.github.repir.tools.Structure.StructuredFileKeyValueRecord;
import java.util.ArrayList;

/**
 * contains the rules to disambiguate between the senses of a word
 *
 * @author jer
 */
public class AOI extends StoredTermFeature<File, Record> {

   public static Log log = new Log(AOI.class);

   protected AOI(Repository repository, String term) {
      super(repository, term);
   }

   @Override
   public File createFile(Datafile datafile) {
      return new File(datafile);
   }

   public ArrayList<Rule> readRules() {
      ArrayList<Rule> list = new ArrayList<Rule>();
      TermString termstring = (TermString) repository.getFeature(TermString.class);
      String term = termstring.readValue(termid);
      for (Record r : this.getKeys()) {
         list.add(new Rule(r, termid));
      }
      return list;
   }

   public Record ceateRecord(Rule rule) {
      Record r = new Record();
      r.sense = rule.sense;
      r.type = rule.type;
      r.words = new int[]{ rule.word0, rule.word1 };
      r.cf = rule.cf;
      r.df = rule.df;
      return r;
   }

   public class File extends StructuredFileKeyValue<Record> {

      public CIntField sense = this.addCInt("sense");
      public CIntField type = this.addCInt("type");
      public CIntArrayField words = this.addCIntArray("words");
      public CIntField cf = this.addCInt("cf");
      public CIntField df = this.addCInt("df");

      public File(Datafile df) {
         super(df);
      }

      @Override
      public Record newRecord() {
         return new Record();
      }

      @Override
      public Record closingRecord() {
         Record r = newRecord();
         r.sense = -1;
         r.type = RuleType.NORULE;
         r.words = new int[0];
         return r;
      }
   }

   public class Record implements StructuredFileKeyValueRecord<File> {

      public int sense;
      public RuleType type;
      public int words[];
      public int cf;
      public int df;

      public String toString() {
         return PrintTools.sprintf("bucketindex=%d term=%s", this.hashCode(), term);
      }

      @Override
      public int hashCode() {
         return MathTools.finishHash(MathTools.combineHash(31, sense));
      }

      @Override
      public boolean equals(Object r) {
         if (r instanceof Record) {
            Record record = (Record) r;
            if (sense == record.sense) {
               return true;
            }
         }
         return false;
      }

      @Override
      public void write(File file) {
         file.sense.write(sense);
         file.type.write(type.ordinal());
         file.words.write(words);
         file.cf.write(cf);
         file.df.write(df);
      }

      @Override
      public void read(File file) {
         sense = file.sense.value;
         type = RuleType.values()[file.type.value];
         words = file.words.value;
         cf = file.cf.value;
         df = file.df.value;
      }

      @Override
      public void convert(StructuredFileKeyValueRecord record) {
         Record r = (Record) record;
         r.sense = sense;
         r.type = type;
         r.words = words;
         r.cf = cf;
         r.df = df;
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
      public final int word0, word1;
      public int cf;
      public int df;

      public Rule(int sense, int termid, RuleType type, int word) {
         this(sense, termid, type, word, -1);
      }

      public Rule(int sense, int termid, RuleType type, int word0, int word1) {
         this.sense = sense;
         this.termid = termid;
         this.type = type;
         this.word0 = word0;
         this.word1 = word1;
      }

      public Rule(Record r, int termid) {
         this(r.sense, termid, r.type, r.words[0], (r.words.length > 1)?r.words[1]:-1);
         cf = r.cf;
         df = r.df;
      }
      
      public Rule setSense(int sense) {
         Rule r = new Rule(sense, termid, type, word0, word1);
         r.cf = cf;
         r.df = df;
         return r;
      }

      public boolean match(int left[], int right[]) {
         switch (type) {
            case LEFT:
               return left.length > 0 && left[0] == word0;
            case RIGHT:
               return right.length > 0 && right[0] == word0;
            case LEFT2:
               return left.length > 1 && left[1] == word0 && left[0] == word1;
            case RIGHT2:
               return right.length > 1 && right[0] == word0 && right[1] == word1;
//            case LEFT3:
//               return left.length > 2 && left[2] == word0 && left[1] == word1 && left[0] == word2;
//            case RIGHT3:
//               return right.length > 2 && right[0] == word0 && right[1] == word1 && right[2] == word2;
            case LR:
               return left.length > 0 && right.length > 0 && left[0] == word0 && right[0] == word1;
            case WINDOW:
               return ArrayTools.contains(word0, left) || ArrayTools.contains(word0, right);
            case WINDOW2:
               for (int i = left.length - 2; i >= 0; i--) {
                  if (left[i] == word1 && left[i + 1] == word0) {
                     return true;
                  }
               }
               for (int i = right.length - 2; i >= 0; i--) {
                  if (right[i] == word0 && right[i + 1] == word1) {
                     return true;
                  }
               }
               if (word0 == termid && right.length > 0 && right[0] == word1) {
                  return true;
               }
               if (word1 == termid && left.length > 0 && left[0] == word0) {
                  return true;
               }
               return false;
            default:
               return false;
         }
      }

      public String toString(Repository r) {
         StringBuilder sb = new StringBuilder();
         TermString ts = (TermString) r.getFeature(TermString.class);
         sb.append(sense).append(" ").append(type).append(" ").append(ts.readValue(word0)).append(" ");
         if (word1 >= 0) {
            sb.append(ts.readValue(word1)).append(" ");
         }
         sb.append(" cf(").append(this.cf).append(")");
         sb.append(" df(").append(this.df).append(")");
         return sb.toString();
      }

      @Override
      public boolean equals(Object o) {
         Rule r = (Rule) o;
         //removed r.sense == sense
         return (r.type == type && word0 == r.word0 && word1 == r.word1);
      }

      @Override
      public int hashCode() {
         return MathTools.finishHash(MathTools.combineHash(
                 MathTools.combineHash(31, type.ordinal()), word0, word1));
      }
   }

   public static class RuleSet extends ArrayList<Rule> {

      Term term;
      public final int width;

      public RuleSet(Repository repository, Term term) {
         width = repository.configuredInt("aoi.width", 10);
         this.term = term;
         AOI termsense = (AOI) repository.getFeature(AOI.class, term.getProcessedTerm());
         termsense.openRead();
         addAll(termsense.readRules());
      }

      public long matchOccurrence(int left[], int right[]) {
         long areas = 0;
         for (Rule r : this) {
            if (r.type != RuleType.NORULE && r.match(left, right)) {
               areas |= (1l << r.sense);
               //log.info("match %d %s", r.sense, Long.toBinaryString(areas));
            }
         }
         //log.info("match %s", Long.toBinaryString(areas));
         return areas;
      }

      public long[] matchAll(int content[]) {
         return matchAll(content, getTermPos(content));
      }

      public long[] matchAll(int content[], int pos[]) {
         long sense[] = new long[pos.length];
         for (int i = 0; i < pos.length; i++) {
            int p = pos[i];
            //log.info("pos %d content.length %d", pos[i], content.length);
            int startleft = (p < width) ? 0 : p - width;
            int endright = (p + width + 1 < content.length) ? p + width + 1 : content.length;
            int left[] = new int[p - startleft];
            int right[] = new int[endright - (p + 1)];
            for (int j = 0; j < left.length; j++) {
               left[j] = content[ p - j - 1];
            }
            System.arraycopy(content, p + 1, right, 0, right.length);
            //log.info("left %s", ArrayTools.concat(left));
            //log.info("right %s", ArrayTools.concat(right));
            sense[i] = matchOccurrence(left, right);
//            for (int b = 0; b < 64; b++) {
//               if (((1l << b) & sense[i]) != 0) {
//                  log.printf("sense %d", b);
//               }
//            }
         }
         return sense;
      }

      public int[] getTermPos(int content[]) {
         ArrayList<Integer> pos = new ArrayList<Integer>();
         for (int p = 0; p < content.length; p++) {
            if (content[p] == term.getID()) {
               pos.add(p);
            }
         }
         return ArrayTools.toIntArray(pos);
      }
   }
}
