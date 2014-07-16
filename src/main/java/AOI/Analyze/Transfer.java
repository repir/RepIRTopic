package AOI.Analyze;

import io.github.repir.Repository.AOI;
import io.github.repir.Repository.Repository;
import io.github.repir.Repository.Term;
import io.github.repir.Repository.TermString;
import io.github.repir.tools.ByteSearch.ByteSearch;
import io.github.repir.tools.Content.HDFSDir;
import java.util.HashMap;
import io.github.repir.tools.Lib.Log;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Retrieve all topics from the TestSet, and store in an output file. arguments:
 * <configfile> <outputfileextension>
 *
 * @author jeroen
 */
public class Transfer {

   public static Log log = new Log(Transfer.class);

   public static void main(String[] args) throws Exception {
      Repository repository = new Repository(args, "target");
      Repository target = new Repository(repository.configuredString("target"));
      TermString termstring = (TermString) repository.getFeature(TermString.class);
      HashMap<Term, Collection<AOI.Record>> list = new HashMap<Term, Collection<AOI.Record>>();
      HashMap<Integer, Integer> translation = new HashMap<Integer, Integer>();
      for (Term term : getKeywords(repository)) {
         AOI aoi = (AOI) repository.getFeature(AOI.class, term.getProcessedTerm());
         if (aoi.getFile().exists() && !term.isStopword()) {
            list.put(term, aoi.getKeys());
         }
      }
      for (Collection<AOI.Record> rulelist : list.values()) {
         for (AOI.Record rule : rulelist) {
            for (int i = 0; i < rule.words.length; i++) {
               int word = rule.words[i];
               if (word > -1) {
                  Integer termid = translation.get(word);
                  if (termid == null) {
                     String term = termstring.readValue(word);
                     termid = target.termToID(term);
                     if (termid < 0) {
                        log.fatal("problem, %s does not exist", term);
                     }
                     translation.put(word, termid);
                  }
                  rule.words[i] = termid;
               }
            }
         }
      }
      for (Map.Entry<Term, Collection<AOI.Record>> entry : list.entrySet()) {
         AOI aoi = (AOI) target.getFeature(AOI.class, entry.getKey().getProcessedTerm());
         aoi.openWrite();
         for (AOI.Record record : entry.getValue()) {
            aoi.write(record);
         }
         aoi.closeWrite();
      }
   }

   public static ArrayList<Term> getKeywords(Repository repository) {
      HDFSDir dir = repository.getBaseDir().getSubdir("dynamic");
      ArrayList<Term> list = new ArrayList<Term>();
      for (String file : dir.matchFiles(ByteSearch.create(repository.getPrefix() + "\\.AOI\\."))) {
         list.add(repository.getProcessedTerm(file.substring(file.lastIndexOf('.') + 1)));
      }
      return list;
   }
}
