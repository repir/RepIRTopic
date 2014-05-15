package testAppend;

import io.github.repir.Repository.EntityStoredFeature;
import io.github.repir.Repository.Pig.PigDoc.File;
import io.github.repir.tools.Content.Datafile;
import io.github.repir.tools.Content.StructuredTextFile;
import io.github.repir.tools.Content.StructuredTextPig;
import io.github.repir.tools.Content.StructuredTextPigTuple;

/**
 * Can store one literal String per Document, e.g. collection ID, title, url.
 * @see EntityStoredFeature
 * @author jer
 */
public class PigDoc extends StructuredTextPig {
      public StructuredTextFile.LongField id = this.addLong("id");
      public StructuredTextFile.StringField collectionid = this.addString("collectionid");

      public PigDoc(Datafile df) {
         super(df);
      }
   
   public static class Tuple {
      public long id;
      public String collectionid;
      
      public Tuple() {}
      
      public void write(PigDoc file) {
         file.id.set(id);
         file.collectionid.set(collectionid);
         file.write();
      }
   }
}
