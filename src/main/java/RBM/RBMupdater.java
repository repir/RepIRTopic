package RBM;

import RBM.RBM.RBMparameterset;
import RBM.RBMtrainingset.Sample;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Lib.Matrix;

/**
 * This class manages the updates during the learning of RBM parameters. Separating this
 * functionality from the RBM allows the creation of alternative learning schemes which can
 * be plugged in to any RBM by overriding {@link RBM#getUpdater() }.
 * <p/>
 * When training an RBM, the {@link #store_weight_updates(RBM.RBMtrainingset.Sample)}
 * should be called after generating the hidden layer for generation 1 and reconstructing
 * the visible and hidden layer for generation 2. The gradients are stored
 * @author jeroen
 */
public class RBMupdater {

   public static Log log = new Log(RBMupdater.class);
   RBM rbm;                   // the RBM that is being learned
   final int num_v, num_h;    // number of visible and hidden nodes
   int count_stored;          // the number of samples seen that have not been updated
   Matrix gradient_v;         // incline between original visible node and reconstructed visible node
   Matrix update_vh_gradient; // for future implementation of momentum
   Matrix update_v_bias;      // sum of gradients for visible nodes of seen samples
   Matrix update_h_bias;      // sum of gradients for hidden nodes of seen samples
   Matrix update_vh;          // sum of gradients for weights of seen samples
   Matrix update_vh_temp;     // temp table
   Matrix update_v_count;     // counts the number of updates per visible node 
   
   public RBMupdater(RBM rbm) {
      this.rbm = rbm;
      num_v = rbm.getNumV();
      num_h = rbm.getNumH();
      gradient_v = new Matrix(1, num_v);
      update_vh_gradient = new Matrix(num_v, num_h);
      update_v_bias = new Matrix(1, num_v);
      update_h_bias = new Matrix(1, num_h);
      update_vh = new Matrix(num_v, num_h);
      update_vh_temp = new Matrix(num_v, num_h);
      update_v_count = new Matrix(1, num_v);
   }

   /**
    * Before calling this, the RBM should have been seeded with the sample, and 
    * generation 1 and 2 for the hidden and visible layers should have been estimated.
    * This method then caches the gradients used to update the parameter set. The 
    * caller can decide how many samples must be processed before the update is
    * carried out by calling {@link #update_weights(RBM.RBM.RBMparameterset, double, double)}. 
    * @param sample The training sample that was used to generate the hidden layer
    * and the second generation of the visible and hidden layer.
    */
   public void store_weight_updates(Sample sample) {
      gradient_v.isMinus(sample.visible, rbm.p_v2);                                // gradient_v = sample.visible - p_v2
      update_vh_temp.isDot(gradient_v.transpose(), rbm.p_h1);                      // update_vh = gradient_v.T * p_h1
      // add only to the gradients of visible nodes if the sample contained a
      // value for that node (i.e. value is not missing)
      for (int v = 0; v < num_v; v++) {
         if (!sample.missing[v]) {
            update_v_count.value[0][v]++;
            for (int h = 0; h < num_h; h++) {
               update_vh.value[v][h] += update_vh_temp.value[v][h];
            }
            update_v_bias.value[0][v] += gradient_v.value[0][v];
         }
      }
      for (int h = 0; h < num_h; h++) {
         update_h_bias.value[0][h] += rbm.p_h1.value[0][h] - rbm.p_h2.value[0][h];
      }
      count_stored++;
   }

   public void store_weight_updates1(Sample sample) {
      gradient_v.isMinus(sample.visible, rbm.p_v2);                                // gradient_v = sample.visible - p_v2
      update_vh_temp.isDot(gradient_v.transpose(), rbm.p_h1);                      // update_vh = gradient_v.T * p_h1
      // add only to the gradients of visible nodes if the sample contained a
      // value for that node (i.e. value is not missing)
      for (int v = 0; v < num_v; v++) {
         if (!sample.missing[v]) {
            update_v_count.value[0][v]++;
            for (int h = 0; h < num_h; h++) {
               update_vh.value[v][h] += update_vh_temp.value[v][h];
            }
         }
      }
      count_stored++;
   }

   public void update_weights(RBMparameterset params, double learning_rate, double momentum) {
      update_weights(params, learning_rate);
   }
   
   /**
    * Updates the weight matrix and the visible and hidden biases, using the 
    * cached gradients.
    * @param params The RBM parameter set to update
    * @param learning_rate A parameter that controls the speed of change
    * @param momentum Not used yet
    */
   public void update_weights(RBMparameterset params, double learning_rate) {
      if (count_stored > 0) {
         // multiply all gadients with the learning_rate
         update_vh.isMult(learning_rate);
         update_h_bias.isMult(learning_rate);
         update_v_bias.isMult(learning_rate);
         
         // add all gradients to the weights/biases
         params.weights_vh.isPlus(params.weights_vh, update_vh);
         params.bias_hidden.isPlus(params.bias_hidden, update_h_bias);
         params.bias_visible.isPlus(params.bias_visible, update_v_bias);
         
         // reset the cached gradients
         update_vh.value = new double[num_v][num_h];
         update_v_bias.value = new double[1][num_v];
         update_h_bias.value = new double[1][num_h];
         count_stored = 0;
      }
   }
}
