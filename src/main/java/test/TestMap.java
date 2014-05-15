package test;

import io.github.repir.Repository.Repository;
import io.github.repir.Repository.Configuration;
import io.github.repir.tools.Lib.Log;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The mapper is generic, and collects data for a query request, using the
 * passed retrieval model, scoring function and query string. The common
 * approach is that each node processes all queries for one index partition. The
 * collected results are reshuffled to one reducer per query where all results
 * for a single query are aggregated.
 * <p/>
 * @author jeroen
 */
public class TestMap extends Mapper<IntWritable, Text, NullWritable, NullWritable> {

   public static Log log = new Log(TestMap.class);
   Configuration conf;
   Repository repository;

   @Override
   protected void setup(Mapper.Context context) throws IOException, InterruptedException {
      repository = new Repository(context.getConfiguration());
      conf = repository.getConfiguration();
   }

   @Override
   public void map(IntWritable inkey, Text invalue, Context context) throws IOException, InterruptedException {
      log.info("term %s", invalue.toString());
      test t = (test)repository.getFeature(test.class);
      t.openWrite();
      test.Record newRecord = t.newRecord();
      newRecord.word = invalue.toString();
      t.write(newRecord);
      t.closeWrite();
   }
}
