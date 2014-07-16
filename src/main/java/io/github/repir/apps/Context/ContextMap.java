package io.github.repir.apps.Context;

import io.github.repir.Repository.DocForward;
import io.github.repir.Repository.Repository;
import io.github.repir.Repository.Term;
import io.github.repir.Repository.TermContext;
import io.github.repir.Repository.TermContext.Doc;
import io.github.repir.Repository.TermContext.Record;
import io.github.repir.Repository.TermContext.Sample;
import io.github.repir.Repository.TermInverted;
import io.github.repir.Retriever.Document;
import io.github.repir.tools.MapReduce.Configuration;
import io.github.repir.tools.Lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
public class ContextMap extends Mapper<IntWritable, Text, NullWritable, NullWritable> {

   public static Log log = new Log(ContextMap.class);
   Configuration conf;
   Repository repository;
   int width = 10;

   @Override
   protected void setup(Mapper.Context context) throws IOException, InterruptedException {
      repository = new Repository(context.getConfiguration());
      conf = repository.getConfiguration();
   }

   @Override
   public void map(IntWritable inkey, Text invalue, Context context) throws IOException, InterruptedException {
      log.info("term %s", invalue.toString());
      TermInverted terminverted = (TermInverted) repository.getFeature(TermInverted.class, "all", invalue.toString());
      Term term = repository.getTerm(invalue.toString());
      if (term.exists()) {
         ArrayList<Sample> samples = new ArrayList<Sample>();
         terminverted.setTerm(term);
         terminverted.setPartition(inkey.get());
         terminverted.setBufferSize(1000000);
         DocForward forward = (DocForward) repository.getFeature(DocForward.class, "all");
         forward.setPartition(inkey.get());
         forward.setBufferSize(1000000);
         forward.openRead();
         terminverted.openRead();
         terminverted.next();
         Document doc = new Document();
         doc.partition = inkey.get();
         while (terminverted.hasNext()) {
            doc.docid = terminverted.docid;
            forward.read(doc);
            int content[] = forward.getValue();
            for (int pos : terminverted.getValue(doc)) {
               int startleft = (pos < width) ? 0 : pos - width;
               int endright = (pos + width + 1 < content.length) ? pos + width + 1 : content.length;
               int left[] = new int[pos - startleft];
               int right[] = new int[endright - (pos + 1)];
               for (int i = 0; i < left.length; i++) {
                  left[i] = content[ pos - i - 1];
               }
               System.arraycopy(content, pos + 1, right, 0, right.length);
               Sample s = new Sample(doc.docid, doc.partition, pos, left, right);
               samples.add(s);
            }
            terminverted.next();
         }
         write(invalue.toString(), inkey.get(), samples);
      }
   }

   public void writeRecord(TermContext termcontext, int partition, Sample s) {
      Record r = (TermContext.Record) termcontext.newRecord();
      r.position = s.pos;
      r.document = (int) s.docid;
      r.partition = partition;
      r.leftcontext = s.leftcontext;
      r.rightcontext = s.rightcontext;
      termcontext.write(r);
   }

   protected void write(String term, int partition, ArrayList<Sample> samples) {
      if (samples.size() > 0) {
         TermContext termcontext = (TermContext) repository.getFeature(TermContext.class, term);
         HashSet<Integer> docids = new HashSet<Integer>();
         HashMap<Doc, ArrayList<Record>> read = termcontext.readRecords();
         termcontext.setBufferSize(10000);
         termcontext.openWrite();
         for (Sample s : samples) {
            writeRecord( termcontext, partition, s);
            docids.add((int) s.docid);
         }
         for (Map.Entry<Doc, ArrayList<Record>> entry : read.entrySet()) {
            if (entry.getKey().partition != partition || !docids.contains(entry.getKey())) {
               for (Record r : entry.getValue()) 
                 termcontext.write(r);
            }
         }
         termcontext.closeWrite();
      }
   }

}
