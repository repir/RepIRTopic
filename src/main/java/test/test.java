package test;

import io.github.repir.Repository.Repository;
import io.github.repir.Repository.StoredDynamicFeature;
import test.test.File;
import test.test.Record;
import io.github.repir.tools.Content.Datafile;
import io.github.repir.tools.Content.StructuredFileKeyValue;
import io.github.repir.tools.Content.StructuredFileKeyValueRecord;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Lib.Log;

/**
 * contains the rules to disambiguate between the senses of a word
 *
 * @author jer
 */
public class test extends StoredDynamicFeature<File, Record> {

   public static Log log = new Log(test.class);

   protected test(Repository repository) {
      super(repository);
   }

   @Override
   public File createFile(Datafile datafile) {
      return new File(datafile);
   }

   public class File extends StructuredFileKeyValue<Record> {

      public StringField word = this.addString("word");

      public File(Datafile df) {
         super(df);
      }

      @Override
      public Record newRecord() {
         return new Record();
      }

      @Override
      public Record closingRecord() {
         Record r = newRecord();
         r.word = "";
         return r;
      }
   }

   public class Record implements StructuredFileKeyValueRecord<File> {

      public String word;

      @Override
      public int hashCode() {
         return word.hashCode();
      }

      @Override
      public boolean equals(Object r) {
         if (r instanceof Record) {
            Record record = (Record) r;
            return word.equals(record.word);
         }
         return false;
      }

      @Override
      public void write(File file) {
         file.word.write(word);
      }

      @Override
      public void read(File file) {
         word = file.word.value;
      }

      @Override
      public void convert(StructuredFileKeyValueRecord record) {
         throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }
   }
}
