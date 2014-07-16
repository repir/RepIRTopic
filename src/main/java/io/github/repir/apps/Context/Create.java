package io.github.repir.apps.Context;

import io.github.repir.Repository.Repository;
import io.github.repir.Repository.Term;
import io.github.repir.Repository.TermContext;
import io.github.repir.Retriever.MapReduce.Retriever;
import io.github.repir.Retriever.Query;
import io.github.repir.TestSet.TestSet;
import io.github.repir.TestSet.Topic.TestSetTopic;
import io.github.repir.tools.MapReduce.Configuration;
import io.github.repir.tools.Lib.Log;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Retrieve all topics from the TestSet, and store in an output file. arguments:
 * <configfile> <outputfileextension>
 *
 * @author jeroen
 */
public class Create {

   public static Log log = new Log(Create.class);

   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration(args, "collection");
      Repository repository = new Repository(conf);
      HashSet<String> keywords = getKeywords(repository, conf.get("collection"));
      ContextJob job = new ContextJob(repository);
      for (String w : keywords) {
         TermContext termcontext = (TermContext) repository.getFeature(TermContext.class, w);
         int termid = repository.termToID(w);
         if (!termcontext.getFile().exists() && !repository.getStopwords().contains(termid)) {
            job.addTerm(w);
         }
      }
      job.submit();
      job.waitForCompletion(true);
   }
   
   // set of terms in the topics of {sets}, hat exists in the target repository.
   public static HashSet<String> getKeywords(Repository target, String... sets) {
      Retriever retriever = new Retriever(target);
      HashSet<String> keywords = new HashSet<String>();
      for (String s : sets) {
         Repository o = new Repository(s);
         TestSet testset = new TestSet(o);
         HashMap<Integer, TestSetTopic> readTopics = TestSet.readTopics(o);
         for (TestSetTopic t : readTopics.values()) {
            Query q = retriever.constructQueryRequest(testset.filterString(t.query));
            for (String w : q.query.split("\\s+")) {
               Term term = o.getTerm(w);
               Term term2 = target.getTerm(w);
               if (term.exists() && term2.exists()) {
                  keywords.add(w);
               }
            }
         }
      }
      return keywords;
   }
}
