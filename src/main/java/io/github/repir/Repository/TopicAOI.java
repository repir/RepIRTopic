package io.github.repir.Repository;

import io.github.repir.Repository.StoredDynamicFeature;
import io.github.repir.Repository.Repository;
import io.github.repir.Repository.TopicAOI.File;
import io.github.repir.Repository.TopicAOI.Record;
import io.github.repir.tools.Content.Datafile;
import io.github.repir.tools.Content.RecordHeaderData;
import io.github.repir.tools.Content.RecordHeaderDataRecord;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Lib.MathTools;

/**
 * @author jer
 */
public class TopicAOI extends StoredDynamicFeature<File, Record> {

   public static Log log = new Log(TopicAOI.class);
   public int zero[] = new int[65];
   
   protected TopicAOI(Repository repository) {
      super(repository);
   }

   @Override
   public File createFile(Datafile df) {
      return new File(df);
   }

   public class File extends RecordHeaderData<Record> {

      public CIntField topic = this.addCInt("topic");
      public CIntField term = this.addCInt("term");
      public CIntField cf = this.addCInt("cf");
      public CIntField df = this.addCInt("df");
      public CIntArrayField senseoccurrence = this.addCIntArray("senseoccurrence");
      public CIntArrayField sensedoccurrence = this.addCIntArray("sensedoccurrence");
      public CIntArrayField sensedf = this.addCIntArray("sensedf");

      public File(Datafile df) {
         super(df);
      }

      @Override
      public Record newRecord() {
         return new Record();
      }
   }

   public class Record implements RecordHeaderDataRecord<File> {
      public int topic;
      public int term;
      public int cf;
      public int df;
      public int senseoccurrence[];
      public int sensedoccurrence[];
      public int sensedf[];
      
      @Override
      public int hashCode() {
         return MathTools.finishHash(MathTools.combineHash(31, term));
      }

      @Override
      public boolean equals(Object r) {
         if (r instanceof Record) {
            Record record = (Record)r;
            if ( topic == record.topic && term == record.term ) {
               return true;
            }
         }
         return false;
      }

      public void write(File file) {
         file.topic.write(topic);
         file.term.write(term);
         file.cf.write(cf);
         file.df.write(df);
         file.senseoccurrence.write(senseoccurrence);
         file.sensedoccurrence.write(sensedoccurrence);
         file.sensedf.write(sensedf);
      }

      public void read(File file) {
         topic = file.topic.value;
         term = file.term.value;
         cf = file.cf.value;
         df = file.df.value;
         senseoccurrence = file.senseoccurrence.value;
         sensedoccurrence = file.sensedoccurrence.value;
         sensedf = file.sensedf.value;
      }

      public void convert(RecordHeaderDataRecord record) {
         Record r = (Record)record;
         r.cf = cf;
         r.df = df;
         r.senseoccurrence = senseoccurrence;
         r.sensedoccurrence = sensedoccurrence;
         r.sensedf = sensedf;
         r.term = term;
         r.topic = topic;
      }
   }
   
   public Record read(int topic, int termid ) {
      this.openRead();
      Record s = (Record)newRecord();
      s.topic = topic;
      s.term = termid;
      Record r = (Record) find(s);
      return r;
   }
}
