package TermInvertedSenseBuilder;

import io.github.repir.Repository.Term;
import io.github.repir.tools.Content.BufferDelayedWriter;
import io.github.repir.tools.Content.BufferReaderWriter;
import io.github.repir.tools.Content.EOCException;
import io.github.repir.tools.Lib.Log;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import static org.apache.hadoop.io.WritableComparator.compareBytes;
import org.apache.hadoop.mapreduce.Partitioner;

public class SenseKey implements WritableComparable<SenseKey> {

   public static Log log = new Log(SenseKey.class);
   public int partition;
   public int termid;
   public int docid;

   public SenseKey() {
   }

   public static SenseKey createKey(int partition, Term term, int docid) {
      SenseKey t = new SenseKey();
      t.partition = partition;
      t.termid = term.getID();
      t.docid = docid;
      return t;
   }

   public int getPartition() {
      return partition;
   }

   @Override
   public void write(DataOutput out) throws IOException {
      BufferDelayedWriter writer = new BufferDelayedWriter();
      writer.write((short) this.partition);
      writer.write(termid);
      writer.write(docid);
      out.write(writer.getAsByteBlock());
   }

   public byte[] writeBytes() {
      BufferDelayedWriter writer = new BufferDelayedWriter();
      writer.write((short) this.partition); // byte 5..6: partition
      writer.write(termid); // byte 8..11 termID
      writer.write(docid);
      return writer.getAsByteBlock();
   }

   // type:byte partition:short bucketindex:long termid:String feature:byte
   @Override
   public void readFields(DataInput in) throws IOException {
      try {
         int length = in.readInt();
         byte b[] = new byte[length];
         in.readFully(b);
         BufferReaderWriter reader = new BufferReaderWriter(b);
         partition = reader.readShort();
         termid = reader.readInt();
         docid = reader.readInt();
      } catch (EOCException ex) {
         throw new IOException(ex);
      }
   }

   @Override
   public int compareTo(SenseKey o) { // never used
      log.crash();
      return 0;
   }

   public static class partitioner extends Partitioner<SenseKey, Writable> {

      @Override
      public int getPartition(SenseKey key, Writable value, int i) {
         return key.partition;
      }
   }

   public static class FirstGroupingComparator
           extends WritableComparator {

      protected FirstGroupingComparator() {
         super(SenseKey.class);
      }

      @Override
      public int compare(byte[] b1, int ss1, int l1, byte[] b2, int ss2, int l2) {
         return compareBytes(b1, ss1 + 4, l1 - 4, b2, ss2 + 4, l2 - 4);
      }
   }

   public static class SecondarySort
           extends WritableComparator {

      protected SecondarySort() {
         super(SenseKey.class);
      }

      @Override
      public int compare(byte[] b1, int ss1, int l1, byte[] b2, int ss2, int l2) {
         return compareBytes(b1, ss1 + 4, l1 - 4, b2, ss2 + 4, l2 - 4);
      }
   }
}
