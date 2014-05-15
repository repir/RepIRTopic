package TermInvertedSenseBuilder;
import io.github.repir.Repository.AOI;
import io.github.repir.Repository.DocForward;
import io.github.repir.Repository.Repository;
import io.github.repir.Repository.Term;
import io.github.repir.Repository.TermInverted;
import io.github.repir.Retriever.Document;
import io.github.repir.Retriever.Retriever;
import io.github.repir.Repository.Configuration;
import io.github.repir.tools.Lib.ArrayTools;
import io.github.repir.tools.Lib.Log; 

/**
 *
 * @author Jeroen Vuurens
 */
public class test {
  public static Log log = new Log( test.class ); 

   public static void main(String[] args) {
      Configuration conf = new Configuration(args, "docid partition term");
      Repository repository = new Repository(conf);
      Retriever retriever = new Retriever(repository);
      int partition = conf.getInt("partition", 0);
      int docid = conf.getInt("docid", 0);
      Term term = repository.getTerm(conf.get("term"));
      AOI.RuleSet rules = new AOI.RuleSet(repository, term);
      DocForward forward = (DocForward)repository.getFeature(DocForward.class, "all");
      forward.setBufferSize(4096 * 1000);
      TermInverted postinglist = (TermInverted)repository.getFeature(TermInverted.class, "all", term.getProcessedTerm());
      postinglist.setPartition(partition);
      postinglist.setTerm( term );
      postinglist.setBufferSize(4096 * 10000);
      postinglist.openRead();
      Document doc = new Document();
      doc.partition = partition;
      while (postinglist.next()) {
         doc.docid = postinglist.docid;
         if (doc.docid == docid) {
         int pos[] = postinglist.getValue(doc);
         log.info("term %s docid %d %s", term.getProcessedTerm(), doc.docid, ArrayTools.concat(pos));
         forward.read(doc);
         int content[] = forward.getValue();
         long sense[] = rules.matchAll(content, pos);
         for (long s : sense) {
            log.info( "%64s", Long.toBinaryString(s));
         }
      }
      }
   }

}
