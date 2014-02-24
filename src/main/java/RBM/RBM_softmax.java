
package RBM;

public class RBM_softmax extends RBM {

   private int K;
   private double softmax_div;
   
   public RBM_softmax(int num_visible_nodes, int num_hidden_nodes) {
      super( num_visible_nodes, num_hidden_nodes );
      setK( 5 );
   }
   
   public void setK( int K ) {
      softmax_div = 0;
      for (int i = 1; i <= K; i++)
         softmax_div += Math.exp(i);
   }
   
   public double sigmoid(double v) {
      return Math.exp(v) / softmax_div;
   }
}
