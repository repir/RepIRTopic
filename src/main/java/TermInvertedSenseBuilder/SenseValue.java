package TermInvertedSenseBuilder;

import io.github.repir.tools.Content.BufferDelayedWriter;
import io.github.repir.tools.Content.BufferReaderWriter;
import io.github.repir.tools.Content.EOCException;
import io.github.repir.tools.Lib.Log;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

public class SenseValue implements Writable {

   public static Log log = new Log(SenseValue.class);
   public BufferReaderWriter reader = new BufferReaderWriter();
   public BufferDelayedWriter writer = new BufferDelayedWriter();
   public int pos[];
   public long sense[];

   public SenseValue() {
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      try {
         reader.readBuffer(in);
         pos = reader.readCIntArray();
         sense = reader.readCLongArray();
      } catch (EOCException ex) {
         throw new IOException(ex);
      }
   }

   @Override
   public void write(DataOutput out) throws IOException {
      writer.writeC(pos);
      writer.writeC(sense);
      out.write( writer.getAsByteBlock() );
   }
}
