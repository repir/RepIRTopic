package RBM;

import io.github.repir.tools.Lib.Log;

public class rbm1 extends RBM {

   public rbm1(int num_visible_nodes, int num_hidden_nodes) {
      super(num_visible_nodes, num_hidden_nodes);
   }

   public RBMupdater getUpdater1() {
      return new RBMupdater_orig( this );
   }
   
   public static void main(String[] args) {
      RBM rbm = new rbm1(6, 4);
      RBMtrainingset set = new RBMtrainingset(new double[][]{
                 {1, 1,  1, 99, 0, 0},
                 {1, 99, 1, 0, 0, 0},
                 {1, 1,  1, 0, 99, 0},
                 {0, 0,  1, 1, 0, 99},
                 {0, 0,  1, 1, 1, 0}}, 99);
      double s = 0;
      for (int i = 0; i < 100; i++)
         s += rbm.train(set, 0.1, 0.1, 500);
      log.printf("sse %f", s / 100);
   }
}
