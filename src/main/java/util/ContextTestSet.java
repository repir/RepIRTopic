package util;

import io.github.repir.tools.Content.Datafile;
import io.github.repir.tools.Lib.HDTools;
import io.github.repir.Repository.Repository;
import io.github.repir.Retriever.Query;
import io.github.repir.RetrieverMR.RetrieverMR;
import io.github.repir.RetrieverMR.RetrieverMRInputFormat;
import io.github.repir.tools.Lib.Log;
import io.github.repir.TestSet.ResultFile;
import java.util.ArrayList;
import java.util.HashSet;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import io.github.repir.Repository.StopwordsCache;
import io.github.repir.Retriever.Retriever;
import io.github.repir.Strategy.Tools.StopWords;
import io.github.repir.TestSet.TestSet;
import io.github.repir.tools.DataTypes.Configuration;
import io.github.repir.tools.Stemmer.englishStemmer;

/**
 * Retrieve all topics from the TestSet, and store in an output file. arguments:
 * <configfile> <outputfileextension>
 *
 * @author jeroen
 */
public class ContextTestSet extends Configured implements Tool {

   public static Log log = new Log(ContextTestSet.class);
   static HashSet<Integer> stopwords;

   public static void main(String[] args) throws Exception {
      Configuration conf = HDTools.readConfig(args, "{othersets}");
      System.exit(HDTools.run(conf, new ContextTestSet(), conf.getStrings("othersets", new String[0])));
   }

   @Override
   public int run(String[] args) throws Exception {
      Repository repository = new Repository((Configuration)getConf());
      RetrieverMR retriever = new RetrieverMR(repository);
      HashSet<String> keywords = new HashSet<String>();
      addTerms( keywords, repository );
      for (String s : args) {
         addTerms( keywords, s );
      }
      for (String w : keywords) {
         Query q = new Query();
         q.stemmedquery = w;
         q.setStrategyClass("ContextRetrievalModel");
         q.performStemming = false;
         retriever.addQueue(q);
      }
      getConf().setInt("ContextTestSet.terms", keywords.size());
      RetrieverMRInputFormat.setSplitable(true);
      RetrieverMRInputFormat.setIndex(repository);
      retriever.retrieveQueue();
      return 0;
   }
   
   public static HashSet<Integer> getStopwords(Repository repository) {
      if (stopwords == null) {
         StopwordsCache swc = (StopwordsCache)repository.getFeature("StopwordsCache");
         stopwords = swc.getStopwords();
      }
      return stopwords;
   }
   
   public static void addTerms( HashSet<String> keywords, String set) {
      Configuration c = HDTools.readConfig(set);
      Repository r = new Repository( c );
      addTerms( keywords, r );
   }
   
   public static void addTerms( HashSet<String> keywords, Repository r) {
      Retriever retriever = new Retriever(r);
      TestSet testset = new io.github.repir.TestSet.TestSet(r);
      for (Query q : testset.getQueries(retriever)) {
         addTerms( keywords, retriever, q);
      }
   }
   
   public static void addTerms( HashSet<String> keywords, Retriever retriever, Query q) {
         retriever.tokenizeQuery(q);
         HashSet<Integer> stopwords = getStopwords( retriever.repository );
         for (String w : q.stemmedquery.split("\\s+")) {
            int termid = retriever.repository.termToID(w);
            if (termid >= 0 && !stopwords.contains(termid)) {
               keywords.add(w);
            }
         }
   }
}
