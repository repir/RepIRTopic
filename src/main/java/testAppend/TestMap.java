package testAppend;

import io.github.repir.tools.Content.Datafile;
import io.github.repir.tools.MapReduce.Configuration;
import io.github.repir.tools.Lib.Log;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import testAppend.PigDoc.Tuple;

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

   @Override
   public void map(IntWritable inkey, Text invalue, Context context) throws IOException, InterruptedException {
      log.info("term %s", invalue.toString());
      PigDoc t = new PigDoc(new Datafile(Configuration.getFS(), "output/tiny3/pigtest"));
      if (t.exists())
         log.info("datafile length %d", t.datafile.getLength());
      t.lock();
      t.openAppend();
      Tuple tuple = new Tuple();
      tuple.id = inkey.get();
      tuple.collectionid = invalue.toString();
      tuple.write(t);
      t.closeWrite();
      t.unlock();
      log.info("datafile length %d", t.datafile.getLength());
   }
}
