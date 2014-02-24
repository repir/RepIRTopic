package RBM;
import java.util.ArrayList;
import io.github.repir.tools.Content.Datafile;
import io.github.repir.tools.Content.RecordCSV;
import io.github.repir.tools.Lib.Log;

public class rbm_jester extends RBM {
   public static Log log = new Log(rbm_jester.class);
   
   public rbm_jester(int num_visible_nodes, int num_hidden_nodes) {
      super( num_visible_nodes, num_hidden_nodes );
   }
   
   public RBMupdater getUpdater1() {
      return new RBMupdater_orig( this );
   }
   
   public static void main(String[] args) {
      RBM rbm = new RBM( 100, 10 );
      JesterIn in = new JesterIn( new Datafile("jester-data-3.csv"));
      in.openRead();
      ArrayList<double[]> users = new ArrayList<double[]>();
      while (in.next()) {
         if (in.votes.value.length == 100) {
            for (int i = 0; i < 100; i++)
               if (in.votes.value[i] != 99)
                  in.votes.value[i] = 0.5 + in.votes.value[i] / 20.0;
            users.add(in.votes.value);
            
         }
      }
      double training_set[][] = users.toArray( new double[users.size()][] );
      RBMtrainingset set = new RBMtrainingset( training_set, 99);
      log.printf("train %d", set.getSize());
      rbm.train( set, 0.1, 0.1, 10);
   }
}

class JesterIn extends RecordCSV {
   RecordCSV.IntField numberofvotes = new IntField("numberofvotes");
   DoubleArrayField votes = new DoubleArrayField("votes"); 
   
   public JesterIn( Datafile df ) {
      super( df );
   }
   
   @Override
   public String createEndFieldTag(Field f) {
      return ",";
   }
}