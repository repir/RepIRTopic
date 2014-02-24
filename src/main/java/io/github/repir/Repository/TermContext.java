package io.github.repir.Repository;

import java.util.ArrayList;
import java.util.HashMap;
import io.github.repir.Repository.TermContext.File;
import io.github.repir.Repository.Repository;
import io.github.repir.Repository.StoredDynamicFeature;
import io.github.repir.Repository.TermContext.Record;
import io.github.repir.tools.Content.Datafile;
import io.github.repir.tools.Content.RecordBinary;
import io.github.repir.tools.Content.RecordHeader;
import io.github.repir.tools.Content.RecordHeaderDataRecord;
import io.github.repir.tools.Content.RecordHeaderRecord;
import io.github.repir.tools.Content.RecordSortHash;
import io.github.repir.tools.Content.RecordSortHashRecord;
import io.github.repir.tools.Lib.ArrayTools;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Lib.MathTools;
import io.github.repir.tools.Lib.PrintTools;

/**
 * contains the captured context of a term.
 * important: the left context is mirrored, e.g. left[0] is the first
 * term on the left of the target word and right[0] is the first term
 * on the right of the target word.
 * @author jer
 */
public class TermContext extends StoredDynamicFeature<File, Record> {

   public static Log log = new Log(TermContext.class);

   protected TermContext(Repository repository) {
      super(repository);
   }

   @Override
   public File createFile(Datafile df) {
      return new File(df);
   }

   public class File extends RecordHeader<Record, Data> {

      public CIntField term = this.addCInt("term");

      public File(Datafile df) {
         super(df);
      }
      
      @Override
      public Record newRecord() {
         return new Record();
      }

      @Override
      protected Data createDatafile(Datafile df) {
         return new Data(df);
      }
   }

   public class Record extends RecordHeaderRecord<File, Data> {

      public int term;
      public int document[];
      public int partition[];
      public int position[];
      public int leftcontext[][];
      public int rightcontext[][];

      public String toString() {
         return PrintTools.sprintf("bucketindex=%d term=%d position=%s document=%s partition=%s\n", this.hashCode(), term, position, document, partition);
      }
      
      @Override
      public int hashCode() {
         return MathTools.finishHash(MathTools.combineHash(31, term));
      }

      @Override
      public boolean equals(Object r) {
         if (r instanceof Record) {
            Record record = (Record)r;
            if ( term == record.term ) {
               return true;
            }
         }
         return false;
      }

      @Override
      public void writeKeys(File file) {
         file.term.write(term);
      }

      @Override
      public void writeData2(Data file) {
         file.position.write(position);
         file.leftcontext.write(leftcontext);
         file.rightcontext.write(rightcontext);
         file.document.write(document);
         file.partition.write(partition);
         position = null;
         leftcontext = null;
         rightcontext = null;
         document = null;
         partition = null;
      }

      @Override
      protected void getKeys(File file) {
         term = file.term.value;
      }

      @Override
      public void getData(Data file) {
         if (file.next()) {
            position = file.position.value;
            leftcontext = file.leftcontext.value;
            rightcontext = file.rightcontext.value;
            document = file.document.value;
            partition = file.partition.value;
         }
      }

      public void convert(RecordHeaderDataRecord record) {
         Record r = (Record) record;
         r.document = document;
         r.leftcontext = leftcontext;
         r.rightcontext = rightcontext;
         r.partition = partition;
         r.position = position;
         r.term = term;
      }
   }
   
   public class Data extends RecordBinary {
      public CIntArrayField position = this.addCIntArray("position");
      public CIntArray2Field leftcontext = this.addCIntArray2("leftcontext");
      public CIntArray2Field rightcontext = this.addCIntArray2("rightcontext");
      public CIntArrayField document = this.addCIntArray("document");
      public CIntArrayField partition = this.addCIntArray("partition");
      
      public Data( Datafile df ) {
         super( df );
      }
   }
   
   public HashMap<Doc, ArrayList<Sample>> read(int termid ) {
      int width = repository.getConfigurationInt("aoi.width", Integer.MAX_VALUE);
      this.openRead();
      Record s = (Record)newRecord();
      s.term = termid;
      Record r = (Record) find(s);
      HashMap<Doc, ArrayList<Sample>> list = null;
      Sample sample;
      log.info("readCache %s %s", s, r);
      if (r != null) {
         list = new HashMap<Doc, ArrayList<Sample>>();
         for (int d = 0; d < r.document.length; d++) {
            Doc doc = new Doc(r.document[d], r.partition[d]);
            ArrayList<Sample> samplelist = list.get(doc);
            if (samplelist == null) {
               samplelist = new ArrayList<Sample>();
               list.put(doc, samplelist);
            }
               int l[] = new int[Math.min(r.leftcontext[d].length, width)];
               System.arraycopy(r.leftcontext[d], 0, l, 0, l.length);
               r.leftcontext[d] = l;
               int ri[] = new int[Math.min(r.rightcontext[d].length, width)];
               System.arraycopy(r.rightcontext[d], 0, ri, 0, ri.length);
               r.rightcontext[d] = ri;
            sample = new Sample( r.position[d], r.leftcontext[d], r.rightcontext[d]);
            
            samplelist.add(sample);
         }
      }
      this.closeRead();
      return list;
   }

   
   public static class Doc implements Comparable<Doc> {

      public int docid;
      public int partition;

      public Doc(int docid, int partition) {
         this.docid = docid;
         this.partition = partition;
      }

      @Override
      public boolean equals(Object obj) {
         Doc d = (Doc) obj;
         return (docid == d.docid && partition == d.partition);
      }

      public int hashCode() {
         return MathTools.finishHash(MathTools.combineHash(31, docid, partition));
      }

      public int compareTo(Doc o) {
         return 1;
      }
   }

   public static class Sample {

      public int pos;
      public long docid;
      public int leftcontext[];
      public int rightcontext[];

      public Sample(int pos, int leftcontext[], int rightcontext[]) {
         this.pos = pos;
         this.leftcontext = leftcontext;
         this.rightcontext = rightcontext;
      }
   }
}
