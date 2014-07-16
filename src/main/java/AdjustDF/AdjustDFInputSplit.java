package AdjustDF;

import TermInvertedSenseBuilder.*;
import io.github.repir.tools.Buffer.BufferDelayedWriter;
import io.github.repir.tools.Buffer.BufferReaderWriter;
import io.github.repir.Repository.Repository;
import io.github.repir.tools.Lib.Log;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * A custom implementation of Hadoop's InputSplit. Each Split holds all Queries
 * that must be processed for a single partition.
 * <p/>
 * @author jeroen
 */
public class AdjustDFInputSplit extends InputSplit implements Writable {

   public static Log log = new Log(AdjustDFInputSplit.class);
   MapInputWritable inputvalue;
   String hosts[]; // preferred node to execute the mapper
   int partition;

   public AdjustDFInputSplit() {
   }

   public AdjustDFInputSplit(Repository repository, int partition, MapInputWritable inputvalue) {
      // if index creation was done properly, a single reducer was used to write all
      // files for a single partition. These files have probably been replicated,
      // so the intersection of hosts indicates the best node to map the split.
      hosts = repository.getPartitionLocation(partition);
      this.partition = partition;
      this.inputvalue = inputvalue;
   }

   /**
    * @return the number of Query requests in this split
    */
   @Override
   public long getLength() throws IOException, InterruptedException {
      return 1;
   }

   @Override
   public void write(DataOutput out) throws IOException {
      BufferDelayedWriter writer = new BufferDelayedWriter();
      writer.write(hosts);
      writer.write(partition);
      inputvalue.write(writer);
      out.write(writer.getAsByteBlock());
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      BufferReaderWriter reader = new BufferReaderWriter(in);
      hosts = reader.readStringArray();
      partition = reader.readInt();
      inputvalue = new MapInputWritable();
      inputvalue.readFields(reader);
   }

   @Override
   public String[] getLocations() throws IOException, InterruptedException {
      return hosts;
   }
}
