package AdjustDF;

import TermInvertedSenseBuilder.*;
import io.github.repir.tools.Content.BufferDelayedWriter;
import io.github.repir.tools.Content.BufferReaderWriter;
import io.github.repir.tools.Content.EOCException;
import io.github.repir.tools.Content.StructureReader;
import io.github.repir.tools.Content.StructureWriter;
import io.github.repir.tools.Lib.Log;
import java.io.DataInput;
import java.io.DataOutput;
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
   public String term;

   public MapInputWritable() {
   }

   public MapInputWritable(int partition, String term) {
      this.partition = partition;
      this.term = term;
   }

   public MapInputWritable clone(int partition) {
      MapInputWritable m = new MapInputWritable();
      m.partition = partition;
      m.term = term;
      return m;
   }

   @Override
   public void write(DataOutput out) throws IOException {
      BufferDelayedWriter writer = new BufferDelayedWriter();
      write(writer);
      writer.writeBuffer(out);
   }

   public void write(StructureWriter writer) {
      writer.write(partition);
      writer.write(term);
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      BufferReaderWriter rw = new BufferReaderWriter();
      rw.readBuffer(in);
      readFields(rw);
   }

   public void readFields(StructureReader reader) throws EOCException {
      partition = reader.readInt();
      term = reader.readString();
   }
}
