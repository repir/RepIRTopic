package io.github.repir.Repository;

import io.github.repir.Repository.DocLiteral;
import io.github.repir.Repository.Repository;
import io.github.repir.Repository.TermDocumentFeature;
import io.github.repir.tools.Content.Datafile;
import io.github.repir.tools.Content.StructuredFileSequential;
import io.github.repir.Repository.TermInvertedSense.File;
import io.github.repir.Retriever.Document;
import io.github.repir.tools.Lib.Log;
import io.github.repir.Repository.TermInvertedSense.SensePos;
import io.github.repir.tools.Lib.PrintTools;

public class TermInvertedSense extends TermDocumentFeature<File, SensePos> {

   public static Log log = new Log(TermInvertedSense.class);
   static final SensePos ZEROPOS = new SensePos();
   DocLiteral collectionid;
   long offsetstart = 0;
   int reducetermid;

   protected TermInvertedSense(Repository repository, String field) {
      super(repository, field);
      collectionid = repository.getCollectionIDFeature();
   }

   public void writeDoc(int termid, int docid, int pos[], long sense[]) { 
      setTerm( termid );
      file.docid.write(docid);
      file.pos.write(pos);
      file.sense.write(sense);
   }

   private void setTerm(int termid) {
      for (; reducetermid < termid; reducetermid++) {
         if (this.reducetermid >= 0) {
            log.info("term %d start %d end %d", reducetermid, offsetstart, file.getOffset());
           file.setOffsetTupleStart(offsetstart);
           file.recordEnd();
         }
         offsetstart = file.getOffset();
     }
   }
   
   public void startWrite( int partition ) {
      reducetermid = -1;
      setPartition(partition);
      getFile().openWrite();
   }
   
   public void finishWrite() {
      for (int term = reducetermid; term < repository.getVocabularySize(); term++) {
         setTerm( term );
      }
      file.closeWrite();
   }

   @Override
   public SensePos getValue(Document doc) {
      if (doc.docid == docid) {
         SensePos p = new SensePos();
         p.pos = file.pos.value;
         p.sense = file.sense.value;
         return p;
      } else {
         return ZEROPOS;
      }
   }

   @Override
   protected int readNextID() {
      if (file.next()) {
         return file.docid.value;
      } else {
         file.docid.value = -1;
      }
      return -1;
   }

   @Override
   public File createFile(Datafile datafile) {
      return new File(datafile);
   }

   @Override
   public void decode(Document d, int reportid) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
   }

   @Override
   public void encode(Document d, int reportid) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
   }

   @Override
   public void report(Document doc, int reportid) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
   }

   @Override
   public SensePos valueReported(Document doc, int reportid) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
   }

   public static class File extends StructuredFileSequential {

      public IntField docid = this.addInt("docid");
      protected CIntIncrField pos = this.addCIntIncr("pos");
      public CLongArrayField sense = this.addCLongArray("sense");

      public File(Datafile df) {
         super(df);
      }

      @Override
      public void hookRecordWritten() {
         // record doesn't end until we say so
      }

      public void recordEnd() {
         super.hookRecordWritten();
      }
   }
   
   public static class SensePos {
      public int pos[];
      public long sense[];
      
      @Override
      public String toString() {
         StringBuilder sb = new StringBuilder();
         for (int i = 0; i < pos.length; i++)
            sb.append(PrintTools.sprintf("\n%8d %64s", pos[i], Long.toBinaryString(sense[i])));
         return sb.toString();
      }
   }
}
