package io.github.repir.Strategy.Collector;

import io.github.repir.Strategy.Collector.CollectorCachable;
import io.github.repir.Strategy.Collector.Collector;
import io.github.repir.Strategy.Collector.CollectorAnalyzer;
import io.github.repir.Repository.AOI;
import java.io.EOFException;
import java.util.ArrayList;
import java.util.Collection;
import io.github.repir.Repository.AOI.Record;
import io.github.repir.Repository.AOI.Rule;
import io.github.repir.Strategy.AOIAnalyzer;
import io.github.repir.Strategy.Term;
import io.github.repir.tools.Content.StructureReader;
import io.github.repir.tools.Content.StructureWriter;
import io.github.repir.tools.Lib.Log;

public class AOICollector extends CollectorAnalyzer<Record> {

   public static Log log = new Log(AOICollector.class);
   AOIAnalyzer termsense;
   Term term;
   int termid;
   public ArrayList<Rule> rules = new ArrayList<Rule>();

   public AOICollector() {
      super();
   }

   public AOICollector(AOIAnalyzer f) {
      super(f);
      termsense = f;
      term = f.getTerm();
      termid = term.termid;
   }

   @Override
   public void startAppend() {
      sdf = getStoredDynamicFeature();
      sdf.openWrite();
   }
   
   
   public void streamappend( ) {
      sdf.write(createRecord());
   }
   
   public void streamappend( Record r ) {
      sdf.write(r);
   }
   
   public void streamappend( CollectorCachable c ) {
      ((AOICollector)c).streamappend( createRecord() );
   }
   
   public Record createRecord() {
      AOI sdf = (AOI)getStoredDynamicFeature();
      Record r = (Record) sdf.newRecord();
      r.term = this.termid;
      r.rules = getRules( rules );
      return r;
   }

   public void addRule(Rule r) {
      log.info("addrule %s", r);
      rules.add(r);
   }

   public static int[][] getRules(ArrayList<Rule> rules) {
      int rulematrix[][] = new int[rules.size()][];
      int count = 0;
      for (Rule rule : rules) {
         rulematrix[count++] = rule.toIntArray();
      }
      return rulematrix;
   }

   @Override
   public AOI getStoredDynamicFeature() {
      AOI aoi = (AOI) this.getRepository().getFeature("AOI");
      return aoi;
   }
   
   @Override
   public String getReducerName() {
      return getCanonicalName();
   }

   @Override
   public Collection<String> getReducerIDs() {
      ArrayList<String> reducers = new ArrayList<String>();
      reducers.add(getReducerName());
      return reducers;
   }

//   @Override
//   public String getKey() {
//      return termid + "";
//   }

   @Override
   public void aggregate(Collector collector) {
      rules.addAll(((AOICollector)collector).rules);
   }

   @Override
   public void writeKey(StructureWriter writer) {
      writer.writeC(termid);
   }

   @Override
   public void readKey(StructureReader reader) throws EOFException {
      termid = reader.readCInt();
   }

   @Override
   public void writeValue(StructureWriter writer) {
      log.info("writeValue %d %d", termid, rules.size());
      writer.writeC(termid);
      writer.write(term.stemmedterm);
      writer.writeC(getRules( rules ));
   }

   @Override
   public void readValue(StructureReader reader) throws EOFException {
      termid = reader.readCInt();
      String termstring = reader.readString();
      int rules[][] = reader.readCIntArray2();
      log.info("readValue %d %s %d", termid, termstring, rules.length);
      for (int sense = 0; sense < rules.length; sense++) {
            Rule r = new Rule( rules[sense] );
            this.rules.add(r);
      }
   }

   @Override
   public boolean equals(Object o) {
      return ((o instanceof AOICollector)
              && ((AOICollector) o).termid == termid);
   }

   @Override
   public int hashCode() {
      return termid;
   }

   @Override
   public void reuse() {
      rules = new ArrayList<Rule>();
   }

}
