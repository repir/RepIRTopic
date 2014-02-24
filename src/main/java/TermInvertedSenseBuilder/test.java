package TermInvertedSenseBuilder;
import static TermInvertedSenseBuilder.SenseBuilderMap.log;
import io.github.repir.Repository.AOI;
import io.github.repir.Repository.DocForward;
import io.github.repir.Repository.Repository;
import io.github.repir.Repository.TermInverted;
import io.github.repir.Retriever.Document;
import io.github.repir.Retriever.Retriever;
import io.github.repir.tools.DataTypes.Configuration;
import io.github.repir.tools.Lib.ArrayTools;
import io.github.repir.tools.Lib.HDTools;
import io.github.repir.tools.Lib.Log; 
import io.github.repir.tools.Stemmer.englishStemmer;

/**
 *
 * @author Jeroen Vuurens
 */
public class test {
  public static Log log = new Log( test.class ); 

   public static void main(String[] args) {
      Configuration conf = HDTools.readConfig(args, "docid partition term");
      Repository repository = new Repository(conf);
      Retriever retriever = new Retriever(repository);
      int partition = conf.getInt("partition", 0);
      int docid = conf.getInt("docid", 0);
      String term = conf.get("term");
      String stemmedterm = englishStemmer.get().stem(term );
      int termid = repository.termToID( stemmedterm );
      AOI.RuleSet rules = new AOI.RuleSet(repository, termid);
      DocForward forward = (DocForward)repository.getFeature("DocForward:all");
      forward.setBufferSize(4096 * 1000);
      TermInverted postinglist = (TermInverted)repository.getFeature("TermInverted:all:" + stemmedterm);
      postinglist.setPartition(partition);
      postinglist.setTerm( stemmedterm );
      postinglist.setBufferSize(4096 * 10000);
      postinglist.openRead();
      Document doc = new Document();
      doc.partition = partition;
      while (postinglist.next()) {
         doc.docid = postinglist.docid;
         if (doc.docid == docid) {
         int pos[] = postinglist.getValue(doc);
         log.info("term %s docid %d %s", stemmedterm, doc.docid, ArrayTools.toString(pos));
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
