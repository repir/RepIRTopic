package TopicAOI;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import io.github.repir.tools.Buffer.BufferDelayedWriter;
import io.github.repir.tools.Buffer.BufferReaderWriter;
import io.github.repir.tools.Content.EOCException;

/**
 *
 * @author Jeroen Vuurens
 */
public class TopicTermSense implements WritableComparable<TopicTermSense> {

   private int topic;
   private int termid;
   private int contextfrequency;
   private int contextdf;
   private int aoicf[];
   private int aoidf[];
   private int df[];

   public TopicTermSense() {
   }

   public TopicTermSense(int topic, int termid) {
      this.topic = topic;
      this.termid = termid;
      aoicf = new int[65];
      aoidf = new int[65];
      df = new int[65];
   }

   public int getTopic() {
      return topic;
   }
   
   public int getTermID() {
      return termid;
   }
   
   public int getContextFrequency() {
      return contextfrequency;
   }
   
   public int getContextDf() {
      return contextdf;
   }
   
   public int[] getAOICf() {
      return aoicf;
   }
   
   public int[] getAOIDf() {
      return aoidf;
   }
   
   public int[] getDf() {
      return df;
   }
   
   @Override
   public int hashCode() {
      int hash = 7;
      hash = 53 * hash + this.topic;
      hash = 53 * hash + this.termid;
      return hash;
   }

   public boolean equals(Object o) {
      return (o instanceof TopicTermSense) && ((TopicTermSense) o).topic == topic && ((TopicTermSense) o).termid == termid;
   }

   public void addSenseDoc(long[] sense) {
      long senseT = 0;
      for (long s : sense) {
         contextfrequency++;
         addSense(s);
         senseT |= s;
      }
      addSenseT( senseT, sense.length );
   }
   
   public void setContextDF( int df ) {
      contextdf = df;
   }

   private void addSense(long s) {
      if (s == 0) {
         this.aoicf[64] ++;
      } else {
         int bit = 0;
         while (s != 0) {
            if ((s & 1) == 1) {
               this.aoicf[bit] ++;
            }
            s >>>= 1;
            bit++;
         }
      }
   }

   private void addSenseT(long s, int freq) {
         int bit = 0;
         while (s != 0) {
            if ((s & 1) == 1) {
               this.aoidf[bit] += freq;
               this.df[bit]++;
            }
            s >>>= 1;
            bit++;
     }
   }

   @Override
   public void write(DataOutput out) throws IOException {
      BufferDelayedWriter writer = new BufferDelayedWriter();
      writer.write(topic);
      writer.write(termid);
      writer.writeC(contextfrequency);
      writer.writeC(contextdf);
      writer.writeC(aoicf);
      writer.writeC(aoidf);
      writer.writeC(df);
      out.write(writer.getAsByteBlock());
   }

   // type:byte partition:short bucketindex:long termid:String feature:byte
   @Override
   public void readFields(DataInput in) throws IOException {
      try {
         BufferReaderWriter reader = new BufferReaderWriter(in);
         topic = reader.readInt();
         termid = reader.readInt();
         contextfrequency = reader.readCInt();
         contextdf = reader.readCInt();
         aoicf = reader.readCIntArray();
         aoidf = reader.readCIntArray();
         df = reader.readCIntArray();
      } catch (EOCException ex) {
         throw new IOException(ex);
      }
   }

   public int compareTo(TopicTermSense o) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
   }

   public static class FirstGroupingComparator extends WritableComparator {

      protected FirstGroupingComparator() {
         super(TopicTermSense.class);
      }

      @Override
      public int compare(byte[] b1, int ss1, int l1, byte[] b2, int ss2, int l2) {
         return compareBytes(b1, ss1 + 4, 8, b2, ss2 + 4, 8);
      }
   }
}
