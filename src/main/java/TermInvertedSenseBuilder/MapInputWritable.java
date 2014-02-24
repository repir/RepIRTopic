package TermInvertedSenseBuilder;

import io.github.repir.tools.Content.BufferDelayedWriter;
import io.github.repir.tools.Content.BufferReaderWriter;
import io.github.repir.tools.Content.StructureReader;
import io.github.repir.tools.Content.StructureWriter;
import io.github.repir.tools.Lib.Log;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;

/**
 * An implementation of Hadoop's {@link WritableComparator} class, that is used
 * to send a {@link IndexReader Query} request to the mapper and the reducer. By
 * default, a separate reducer is created for each query, and results are
 * combined by comparing the unique query id.
 * <p/>
 * @author jeroen
 */
public class MapInputWritable implements Writable {

   public static Log log = new Log(MapInputWritable.class);
   public int partition;
   public String stemmedterm;

   public MapInputWritable() {
   }

   public MapInputWritable(int partition, String term) {
      this.partition = partition;
      this.stemmedterm = term;
   }

   @Override
   public void write(DataOutput out) throws IOException {
      BufferDelayedWriter writer = new BufferDelayedWriter();
      write(writer);
      writer.writeBuffer(out);
   }

   public void write(StructureWriter writer) {
      writer.write(partition);
      writer.write(stemmedterm);
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      BufferReaderWriter rw = new BufferReaderWriter();
      rw.readBuffer(in);
      readFields(rw);
   }

   public void readFields(StructureReader reader) throws EOFException {
      partition = reader.readInt();
      stemmedterm = reader.readString();
   }
}
