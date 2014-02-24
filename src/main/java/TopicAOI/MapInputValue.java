package TopicAOI;

import TopicAOI.MapInputValue.TopicKey;
import io.github.repir.tools.Content.BufferDelayedWriter;
import io.github.repir.tools.Content.BufferReaderWriter;
import io.github.repir.tools.Content.StructureReader;
import io.github.repir.tools.Content.StructureWriter;
import io.github.repir.tools.Lib.Log;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import org.apache.hadoop.io.Writable;
import io.github.repir.tools.DataTypes.HashMap;

/**
 * @author jeroen
 */
public class MapInputValue implements Writable {

   public static Log log = new Log(MapInputValue.class);
   public int partition;
   public int topic;
   public int termid;
   public String stemmedterm;
   public HashSet<String> documents;

   public MapInputValue() {
   }
   
   public MapInputValue clone( int partition ) {
      MapInputValue m = new MapInputValue();
      m.partition = partition;
      m.topic = topic;
      m.termid = termid;
      m.stemmedterm = stemmedterm;
      m.documents = (HashSet<String>)documents.clone();
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
      writer.write(topic);
      writer.write(termid);
      writer.write(stemmedterm);
      writer.writeStr(documents);
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      BufferReaderWriter rw = new BufferReaderWriter();
      rw.readBuffer(in);
      readFields(rw);
   }

   public void readFields(StructureReader reader) throws EOFException {
      partition = reader.readInt();
      topic = reader.readInt();
      termid = reader.readInt();
      stemmedterm = reader.readString();
      documents = new HashSet<String>(reader.readStrArrayList());
   }

   public static class TopicKey {

      int termid;
      int topic;

      protected TopicKey() {}
      
      public TopicKey(int topic, int termid) {
         this.topic = topic;
         this.termid = termid;
      }

      public void readFields(StructureReader reader) throws EOFException {
         topic = reader.readInt();
         termid = reader.readInt();
      }

      public void write(StructureWriter writer) {
         writer.write(topic);
         writer.write(termid);
      }
   }
}
