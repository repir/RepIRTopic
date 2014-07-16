package AOI.Analyze;

import io.github.repir.Repository.AOI;
import io.github.repir.Repository.Repository;
import io.github.repir.Repository.Term;
import io.github.repir.tools.hadoop.Configuration;
import io.github.repir.tools.Lib.Log;
import java.io.IOException;
import java.util.ArrayList;
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
public class AnalyzeMap extends Mapper<IntWritable, Text, NullWritable, NullWritable> {

   public static Log log = new Log(AnalyzeMap.class);
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
      Term term = repository.getTerm(invalue.toString());
      if (term.exists()) {
         AOIAna ana = new AOIAna(repository, term);
         ArrayList<AOI.Rule> rules = ana.process();
         AOI aoi = (AOI)repository.getFeature(AOI.class, term.getProcessedTerm());
         aoi.openWrite();
         for (AOI.Rule rule : rules) {
            aoi.write(aoi.ceateRecord(rule));
         }
         aoi.closeWrite();
      }
   }
}
