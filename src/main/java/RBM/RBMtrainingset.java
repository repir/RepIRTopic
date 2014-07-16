package RBM;
import RBM.RBMtrainingset.Sample;
import java.util.Iterator;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Lib.Matrix;
import io.github.repir.tools.Lib.RandomTools;

public class RBMtrainingset implements Iterable<Sample> {
   public static Log log = new Log(RBMtrainingset.class);
   int randomroworder[];
   int row = Integer.MAX_VALUE;
   int num_v;
   public Sample samples[];
   
   public RBMtrainingset(double data[][], double missing) {
      this.samples = new Sample[ data.length ];
      for (int i = 0; i < data.length; i++) {
         samples[i] = new Sample( data[i], missing );
      }
      num_v = samples[0].missing.length;
   }
   
   public int getSize() {
      return samples.length;
   }

   public Iterator<Sample> iterator() {
      return new TrainingSetIterator();
   }
   
   public Iterable<Sample> randomIterable() {
      if (row >= samples.length) {
         randomroworder = RandomTools.getRandomList(samples.length);
         row = 0;
      }
      return new RandomTrainingSetIterator();
   }
   
   public boolean atEndOfRandomSets() {
      return row == samples.length;
   }
   
   
   
   class TrainingSetIterator implements Iterator<Sample> {
      int row = 0;
      
      public boolean hasNext() {
         return row < samples.length;
      }

      public Sample next() {
         if (hasNext())
            return samples[row++];
         else
            return null;
      }

      public void remove() {
         throw new UnsupportedOperationException("Not supported yet.");
      }
   }
   
   class RandomTrainingSetIterator implements Iterator<Sample>,Iterable<Sample> {
      int lastrow;
      
      public RandomTrainingSetIterator() {
         lastrow = Math.min( row + num_v, samples.length);
      }
      
      public boolean hasNext() {
         return row < lastrow;
      }

      public Sample next() {
         if (row < lastrow)
            return samples[randomroworder[row++]];
         else
            return null;
      }

      public void remove() {
         throw new UnsupportedOperationException("Not supported yet.");
      }

      public Iterator<Sample> iterator() {
         return this;
      }
   }
   
   public class Sample {
      public Matrix visible;
      public boolean containsmissing;
      public boolean missing[];
      
      public Sample( double values[], double missingvalue ) {
         visible = new Matrix( values );
         missing = new boolean[ values.length ];
         for (int i = 0; i < values.length; i++) {
            if (values[i] == missingvalue) {
               missing[i] = true;
               containsmissing = true;
            }
         }
      }
   }
}
