package AdjustDF;

import io.github.repir.tools.Buffer.BufferDelayedWriter;
import io.github.repir.tools.Buffer.BufferReaderWriter;
import io.github.repir.tools.Content.EOCException;
import io.github.repir.tools.Lib.Log;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class SenseValue implements Writable {

   public static Log log = new Log(SenseValue.class);
   public BufferReaderWriter reader = new BufferReaderWriter();
   public BufferDelayedWriter writer = new BufferDelayedWriter();
   public long sense;
   public int freq;

   public SenseValue() {
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      try {
         reader.readBuffer(in);
         sense = reader.readCLong();
         freq = reader.readCInt();
      } catch (EOCException ex) {
         throw new IOException(ex);
      }
   }

   @Override
   public void write(DataOutput out) throws IOException {
      writer.writeC(sense);
      writer.writeC(freq);
      out.write( writer.getAsByteBlock() );
   }
}
