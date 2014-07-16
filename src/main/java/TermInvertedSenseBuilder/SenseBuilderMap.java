package TermInvertedSenseBuilder;

import io.github.repir.Repository.AOI.RuleSet;
import io.github.repir.Repository.DocForward;
import io.github.repir.Repository.Repository;
import io.github.repir.Repository.Term;
import io.github.repir.Repository.TermInverted;
import io.github.repir.Retriever.Document;
import io.github.repir.tools.Lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SenseBuilderMap extends Mapper<NullWritable, Text, SenseKey, SenseValue> {

    public static Log log = new Log(SenseBuilderMap.class);
    private Repository repository;
    private SBInputSplit filesplit;
    SenseKey outkey;
    SenseValue outvalue = new SenseValue();
    ArrayList<term> terms = new ArrayList<term>();
    int size = 0;

   @Override

    protected void setup(Context context) throws IOException, InterruptedException {
        repository = new Repository(context.getConfiguration());
        filesplit = ((SBInputSplit) context.getInputSplit());
    }

    @Override
    public void map(NullWritable inkey, Text value, Context context) throws IOException, InterruptedException {
        log.info("Term %s", value);
        term t = new term(value.toString());
        terms.add(t);
        size += t.size();
        if (size > 1000000000)
            process(context);
        context.progress();
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (terms.size() > 0)
            process(context);
    }

    
    public void process(Context context) throws InterruptedException, IOException {
        TreeSet<Integer> docs = new TreeSet<Integer>();
        for (term t : terms) {
            docs.addAll(t.postings.keySet());
        }
        DocForward forward = (DocForward) repository.getFeature(DocForward.class, "all");
        forward.setPartition(filesplit.partition);
        forward.setBufferSize(100000000);
        for (int docid : docs) {
            if (docid % 10000 == 0)
               log.info("process %d", docid);
            forward.setOffset(docid);
            forward.next();
            int content[] = forward.getValue();
            for (term t : terms) {
                int pos[] = t.postings.get(docid);
                if (pos != null) {
                   long sense[] = t.rules.matchAll(content, pos);
                   outvalue.pos = pos;
                   outvalue.sense = sense;
                   outkey = SenseKey.createKey(filesplit.partition, t.term, docid);
                   context.write(outkey, outvalue);
                }
            }
        }
        forward.closeRead();
        terms = new ArrayList<term>();
        size = 0;
    }

    class term {

        Term term;
        RuleSet rules;
        HashMap<Integer, int[]> postings = new HashMap<Integer, int[]>();
        int size = -1;

        public term(String term) {
            this.term = repository.getProcessedTerm(term);
            rules = new RuleSet(repository, this.term);
            TermInverted postinglist = (TermInverted) repository.getFeature(TermInverted.class, "all", term);
            postinglist.setPartition(filesplit.partition);
            postinglist.setTerm(this.term);
            postinglist.readResident();
            Document doc = new Document();
            doc.partition = filesplit.partition;
            while (postinglist.next()) {
                doc.docid = postinglist.docid;
                postings.put(postinglist.docid, postinglist.getValue(doc));
            }
            postinglist.closeRead();
        }
        
        public int size() {
           if (size < 0) {
              size = 44 * rules.size();
              size += postings.size() * 20;
              for (int[] i : postings.values())
                  size += 20 + 4*i.length;
           }
           return size;
        }
    }
}

