package RBM;

import io.github.repir.tools.Lib.Log;

public class rbmb extends RBM2 {

   public rbmb(int num_visible_nodes, int num_hidden_nodes) {
      super(num_visible_nodes, num_hidden_nodes);
   }
   
   public static void main(String[] args) {
      RBM2 rbm = new rbmb(6, 4);
      RBMtrainingset set = new RBMtrainingset(new double[][]{
                 {1, 1,  1, 99, 0, 0},
                 {1, 99, 1, 0, 0, 0},
                 {1, 1,  1, 0, 99, 0},
                 {0, 0,  1, 1, 0, 99},
                 {0, 0,  1, 1, 1, 0}}, 99);
      rbm.train(set, 0.1, 0.1, 500);
   }
}
