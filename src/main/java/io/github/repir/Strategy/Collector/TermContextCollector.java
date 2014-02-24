package io.github.repir.Strategy.Collector;
import io.github.repir.Strategy.Collector.Collector;
import io.github.repir.Strategy.Collector.CollectorCachable;
import io.github.repir.Repository.TermContext;
import java.io.EOFException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import io.github.repir.Repository.TermContext.Record;
import io.github.repir.Retriever.Document;
import io.github.repir.Repository.TermContext.Doc;
import io.github.repir.Repository.TermContext.Sample;
import io.github.repir.Strategy.Term;
import io.github.repir.Strategy.TermContextFeature;
import io.github.repir.tools.Content.StructureReader;
import io.github.repir.tools.Content.StructureWriter;
import io.github.repir.tools.Lib.Log;

public class TermContextCollector extends CollectorCachable<Record> {
   public static Log log = new Log(TermContextCollector.class);
   public int width = 10;
   TermContextFeature termcontextfeature;
   Term term;
   int termid;
   public HashMap<Doc, ArrayList<Sample>> samples = new HashMap<Doc, ArrayList<Sample>>();
  
   public TermContextCollector( ) {
      super();
   }

   public TermContextCollector(TermContextFeature f) {
      super(f.root.retrievalmodel);
      containedfeatures.add(f);
      termcontextfeature = f;
      term = f.term;
      termid = term.termid;
      width = this.retriever.repository.getConfigurationInt("aoi.width", 10);
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
      ((TermContextCollector)c).streamappend( createRecord() );
   }
   
   public Record createRecord() {
      TermContext sdf = getStoredDynamicFeature();
      Record r = (Record) sdf.newRecord();
      r.term = this.termid;
      int size = getSampleSize();
      r.position = new int[size];
      r.document = new int[size];
      r.partition = new int[size];
      r.leftcontext = new int[size][];
      r.rightcontext = new int[size][];
      int i = 0;
      for (Map.Entry<Doc, ArrayList<Sample>> entry : samples.entrySet()) {
         for (Sample s : entry.getValue()) {
            r.position[i] = s.pos;
            r.document[i] = entry.getKey().docid;
            r.partition[i] = entry.getKey().partition;
            r.leftcontext[i] = s.leftcontext;
            r.rightcontext[i] = s.rightcontext;
            i++;
         }
      }
      return r;
   }

   public int getSampleSize() {
      int size = 0;
      for (ArrayList<Sample> s : samples.values())
         size += s.size();
      return size;
   }
   
   @Override
   public TermContext getStoredDynamicFeature() {
      TermContext termcontext = (TermContext) this.getRepository().getFeature("TermContext");
      return termcontext;
   }

   public Collection<String> getReducerIDs() {
         ArrayList<String> reducers = new ArrayList<String>();
         reducers.add(this.getCanonicalName());
         return reducers;
   }


   @Override
   public boolean reduceInQuery() {
      return false;
   }

   @Override
   public void setCollectedResults() {
      // no need
   }
   
   @Override
   public void collectDocument(Document doc) {
      //log.info("%s", termcontextfeature.docforward);
      int content[] = termcontextfeature.docforward.getValue();
      Doc d = new Doc(doc.docid, doc.partition);
      ArrayList<Sample> sample = new ArrayList<Sample>();
      for (int pos : term.featurevalues.pos) {
         int startleft = (pos < width)?0:pos - width;
         int endright = (pos + width + 1 < content.length)?pos + width + 1:content.length;
         int left[] = new int[ pos - startleft ];
         int right[] = new int[ endright - (pos + 1) ];
         for (int i = 0; i < left.length; i++)
            left[i] = content[ pos - i - 1];
         System.arraycopy(content, pos+1, right, 0, right.length);
         Sample s = new Sample(pos, left, right);
         sample.add(s);
      }
      if (sample.size() > 0)
         samples.put(d, sample);
   }

   @Override
   public void aggregate(Collector collector) {
      samples.putAll(((TermContextCollector)collector).samples);
   }

   @Override
   public void decode() {
      throw new UnsupportedOperationException("Not supported yet.");
   }

   @Override
   public void writeKey(StructureWriter writer) {
      writer.writeC(termid);
      //log.info("writekey %d", termid);
   }

   @Override
   public void readKey(StructureReader reader) throws EOFException {
      termid = reader.readCInt();
      //log.info("readkey %d", termid);
   }

   @Override
   public void writeValue(StructureWriter writer) {
      writer.writeC( samples.size() );
      for (Map.Entry<Doc, ArrayList<Sample>> e : samples.entrySet()) {
         writer.writeC( e.getKey().docid );
         writer.writeC( e.getKey().partition );
         writer.writeC( e.getValue().size() );
         for ( Sample s : e.getValue() ) {
            writer.writeC( s.pos );
            writer.writeC( s.leftcontext );
            writer.writeC( s.rightcontext );
         }
      }
   }

   @Override
   public void readValue(StructureReader reader) throws EOFException {
      int docs = reader.readCInt();
      for (int d = 0; d < docs; d++) {
         Doc doc = new Doc( reader.readCInt(), reader.readCInt() );
         ArrayList<Sample> sample = new ArrayList<Sample>();
         int countsamples = reader.readCInt();
         for (int i = 0; i < countsamples; i++) {
            int pos = reader.readCInt();
            int left[] = reader.readCIntArray();
            int right[] = reader.readCIntArray();
            sample.add( new Sample( pos, left, right ));
         }
         samples.put(doc, sample);
      }
   }

    @Override
    public void prepareRetrieval() {
        termcontextfeature.doPrepareRetrieval();
    }

   @Override
   public boolean equals(Object o) {
      return ((o instanceof TermContextCollector) &&
              ((TermContextCollector)o).termid == termid);
   }

   @Override
   public int hashCode() {
      return termid;
   }

   @Override
   public String getReducerName() {
      return this.getCanonicalName();
   }

   @Override
   public void reuse() {
      samples = new HashMap<Doc, ArrayList<Sample>>();
   }
}
