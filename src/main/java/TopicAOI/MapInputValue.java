package TopicAOI;

import io.github.repir.tools.Buffer.BufferDelayedWriter;
import io.github.repir.tools.Buffer.BufferReaderWriter;
import io.github.repir.tools.Content.EOCException;
import io.github.repir.tools.Structure.StructureReader;
import io.github.repir.tools.Structure.StructureWriter;
import io.github.repir.tools.DataTypes.Tuple2;
import io.github.repir.tools.Lib.Log;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Writable;

/**
 * @author jeroen
 */
public class MapInputValue implements Writable {

   public static Log log = new Log(MapInputValue.class);
   public int partition;
   public HashMap<Tuple2<Integer, String>, ArrayList<String>> map_topicterm_documents;

   public MapInputValue() {
   }
   
   public MapInputValue clone( int partition ) {
      MapInputValue m = new MapInputValue();
      m.partition = partition;
      m.map_topicterm_documents = map_topicterm_documents;
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
      writer.write(map_topicterm_documents.size());
      for (Map.Entry<Tuple2<Integer, String>, ArrayList<String>> entry : map_topicterm_documents.entrySet()) {
         writer.write(entry.getKey().value1);
         writer.write(entry.getKey().value2);
         writer.writeStr(entry.getValue());
      }
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      BufferReaderWriter rw = new BufferReaderWriter();
      rw.readBuffer(in);
      readFields(rw);
   }

   public void readFields(StructureReader reader) throws EOCException {
      partition = reader.readInt();
      map_topicterm_documents = new HashMap<Tuple2<Integer, String>, ArrayList<String>>();
      int size = reader.readInt();
      for (int i = 0 ; i < size; i++) {
         map_topicterm_documents.put(
                 new Tuple2<Integer, String>(reader.readInt(), reader.readString()),
                 reader.readStrArrayList());
      }
   }
}
