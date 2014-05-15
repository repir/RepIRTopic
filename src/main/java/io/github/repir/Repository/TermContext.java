package io.github.repir.Repository;

import io.github.repir.Repository.TermContext.File;
import io.github.repir.Repository.TermContext.Record;
import io.github.repir.tools.Content.Datafile;
import io.github.repir.tools.Content.StructuredFileKeyValue;
import io.github.repir.tools.Content.StructuredFileKeyValueRecord;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Lib.MathTools;
import io.github.repir.tools.Lib.PrintTools;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * contains the captured context of a term. important: the left context is
 * mirrored, e.g. left[0] is the first term on the left of the target word and
 * right[0] is the first term on the right of the target word.
 *
 * @author jer
 */
public class TermContext extends StoredTermFeature<File, Record> {

   public static Log log = new Log(TermContext.class);

   protected TermContext(Repository repository, String term) {
      super(repository, term);
   }

   @Override
   public File createFile(Datafile df) {
      return new File(df);
   }

   public class File extends StructuredFileKeyValue<Record> {

      public CIntField position = this.addCInt("position");
      public CIntArrayField leftcontext = this.addCIntArray("leftcontext");
      public CIntArrayField rightcontext = this.addCIntArray("rightcontext");
      public CIntField document = this.addCInt("document");
      public CIntField partition = this.addCInt("partition");

      public File(Datafile df) {
         super(df);
      }

      @Override
      public Record newRecord() {
         return new Record();
      }

      @Override
      public Record closingRecord() {
         Record r = new Record();
         r.position = -1;
         return r;
      }
   }

   public class Record implements StructuredFileKeyValueRecord<File> {

      public int document;
      public int partition;
      public int position;
      public int leftcontext[];
      public int rightcontext[];

      public String toString() {
         return PrintTools.sprintf("bucketindex=%d term=%d position=%s document=%s partition=%s\n", this.hashCode(), term, position, document, partition);
      }
      
      @Override
      public int hashCode() {
         return MathTools.finishHash(MathTools.combineHash(31, document, partition, position));
      }

      @Override
      public boolean equals(Object r) {
         if (r instanceof Record) {
            Record record = (Record)r;
            if ( document == record.document && partition == record.partition && position == record.position ) {
               return true;
            }
         }
         return false;
      }

      public void convert(StructuredFileKeyValueRecord record) {
         Record r = (Record) record;
         r.document = document;
         r.leftcontext = leftcontext;
         r.rightcontext = rightcontext;
         r.partition = partition;
         r.position = position;
      }

      @Override
      public void write(File file) {
         file.position.write(position);
         file.leftcontext.write(leftcontext);
         file.rightcontext.write(rightcontext);
         file.document.write(document);
         file.partition.write(partition);
      }

      @Override
      public void read(File file) {
         position = file.position.value;
         leftcontext = file.leftcontext.value;
         rightcontext = file.rightcontext.value;
         document = file.document.value;
         partition = file.partition.value;
      }
   }

   public HashMap<Doc, ArrayList<Sample>> readSamples() {
      int width = repository.configuredInt("aoi.width", Integer.MAX_VALUE);
      HashMap<Doc, ArrayList<Sample>> list = new HashMap<Doc, ArrayList<Sample>>();
      for (Map.Entry<Doc, ArrayList<Record>> entry : readRecords().entrySet()) {
         ArrayList<Sample> samplelist = list.get(entry.getKey());
         if (samplelist == null) {
            samplelist = new ArrayList<Sample>();
            list.put(entry.getKey(), samplelist);
         }
         for (Record r : entry.getValue()) {
            int l[] = new int[Math.min(r.leftcontext.length, width)];
            System.arraycopy(r.leftcontext, 0, l, 0, l.length);
            r.leftcontext = l;
            int ri[] = new int[Math.min(r.rightcontext.length, width)];
            System.arraycopy(r.rightcontext, 0, ri, 0, ri.length);
            r.rightcontext = ri;
            Sample sample = new Sample(r.document, r.partition, r.position, r.leftcontext, r.rightcontext);
            samplelist.add(sample);
         }
      }
      return list;
   }

   public HashMap<Doc, ArrayList<Record>> readRecords() {
      HashMap<Doc, ArrayList<Record>> list = new HashMap<Doc, ArrayList<Record>>();
      for (Record r : getKeys()) {
         Doc doc = new Doc(r.document, r.partition);
         ArrayList<Record> recordlist = list.get(doc);
         if (recordlist == null) {
            recordlist = new ArrayList<Record>();
            list.put(doc, recordlist);
         }
         recordlist.add(r);
      }
      closeRead();
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

      public Sample(int docid, int partition, int pos, int leftcontext[], int rightcontext[]) {
         this.docid = docid | (partition << 32);
         this.pos = pos;
         this.leftcontext = leftcontext;
         this.rightcontext = rightcontext;
      }
   }
}
