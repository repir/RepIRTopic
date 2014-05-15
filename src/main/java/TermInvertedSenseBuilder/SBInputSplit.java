package TermInvertedSenseBuilder;

import io.github.repir.Repository.Repository;
import io.github.repir.tools.Content.BufferDelayedWriter;
import io.github.repir.tools.Content.BufferReaderWriter;
import io.github.repir.tools.Lib.Log;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * A custom implementation of Hadoop's InputSplit. Each Split holds all Queries
 * that must be processed for a single partition.
 * <p/>
 * @author jeroen
 */
public class SBInputSplit extends InputSplit implements Writable {

   public static Log log = new Log(SBInputSplit.class);
   ArrayList<Text> inputvalue = new ArrayList<Text>();
   String hosts[]; // preferred node to execute the mapper
   int partition;

   public SBInputSplit() {
   }

   public SBInputSplit(Repository repository, int partition) {
      // if index creation was done properly, a single reducer was used to write all
      // files for a single partition. These files have probably been replicated,
      // so the intersection of hosts indicates the best node to map the split.
      hosts = repository.getPartitionLocation(partition);
      this.partition = partition;
   }
   
   public void add( Text v ) {
      inputvalue.add(v);
   }

   /**
    * @return the number of Query requests in this split
    */
   @Override
   public long getLength() throws IOException, InterruptedException {
      return inputvalue.size();
   }

   @Override
   public void write(DataOutput out) throws IOException {
      BufferDelayedWriter writer = new BufferDelayedWriter();
      writer.write(hosts);
      writer.write(partition);
      writer.write(inputvalue.size());
      for (Text v : inputvalue)
         writer.write(v.toString());
      out.write(writer.getAsByteBlock());
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      BufferReaderWriter reader = new BufferReaderWriter(in);
      hosts = reader.readStringArray();
      partition = reader.readInt();
      int size = reader.readInt();
      for (int i = 0; i < size; i++) {
         Text v = new Text();
         v.set(reader.readString());
         inputvalue.add(v);
      }
   }

   @Override
   public String[] getLocations() throws IOException, InterruptedException {
      return hosts;
   }
}
