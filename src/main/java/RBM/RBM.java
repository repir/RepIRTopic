package RBM;

import RBM.RBMtrainingset.Sample;
import io.github.repir.tools.Lib.ArrayTools;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Lib.Matrix;
import io.github.repir.tools.Lib.RandomTools;

/**
 * Restricted Boltzmann Machine, that estimates latent concepts, minimizing the
 * error rate in reconstructing the original training samples.
 */
public class RBM {

   public static Log log = new Log(RBM.class);

   public class RBMparameterset {

      Matrix weights_vh;                // array num_h * num_v that contain the weights between hidden and visible nodes
      Matrix bias_visible;
      Matrix bias_hidden;
      double sse = Double.MAX_VALUE; // sum of squared error over the reconstructed visible output

      public RBMparameterset() {
         weights_vh = new Matrix(num_v, num_h);
         bias_visible = new Matrix(1, num_v);
         bias_hidden = new Matrix(1, num_h);
      }

      public RBMparameterset(RBMparameterset set) {
         this();
         set.weights_vh.copy(weights_vh);
         set.bias_hidden.copy(bias_hidden);
         set.bias_visible.copy(bias_visible);
         sse = set.sse;
      }
   }
   final int num_h;                 // number of hidden nodes
   final int num_v;                 // number of visible nodes
   int model_improve_freedom = 20;
   // arrays to contain the activation probability and states of 2 RBM generations   
   Matrix p_h1;                 // activation probability for the hidden nodes in generation 1    
   Matrix state_h1;             // binary states for the hidden nodes in generation 1
   Matrix p_v2;                 // reconstructed values for the visible nodes in generation 2
   Matrix p_vtemp;              // temp storage of reconstructed values
   Matrix p_h2;                 // activation probability for the hidden nodes in generation 2
   Matrix state_h2;             // binary states for the hidden nodes in generation 2
   RBMparameterset params;      // set of RBM parameters
   RBMupdater updater; // collects the training data and updates the parameters

   /**
    * @param num_visible_nodes the fixed number of input features for every
    * sample
    * @param num_hidden_nodes the fixed number of latent concepts used
    */
   public RBM(int num_visible_nodes, int num_hidden_nodes) {
      this.num_h = num_hidden_nodes;
      this.num_v = num_visible_nodes;

      params = new RBMparameterset();
      params.weights_vh.fillRandom(0.2); // Seed with a Gaussian distribution with mean 0 and standard deviation 0.1. 

      updater = getUpdater();

      p_h1 = new Matrix(1, num_h);
      state_h1 = new Matrix(1, num_h);
      p_v2 = new Matrix(1, num_v);
      p_vtemp = new Matrix(1, num_v);
      p_h2 = new Matrix(1, num_h);
      state_h2 = new Matrix(1, num_h);
   }

   public int getNumV() {
      return num_v;
   }

   public int getNumH() {
      return num_h;
   }

   public RBMupdater getUpdater() {
      return new RBMupdater(this);
   }

   /**
    * Train the machine
    * <p/>
    * @param training_set Each row is a training example consisting of the
    * values of the visible features/nodes.
    * @param learning_rate multiplier used to increase the weights based on the
    * gradient of descend
    * @param momentum accelerator of the learning process
    * @param max_epochs maximum number of iterations
    */
   public double train(RBMtrainingset set, double learning_rate, double momentum, int max_epochs) {
      /**
       * For each epoch, the whole training set is traversed, summing the
       * squared error (SSE) between input and reconstructed input. If the SSE
       * is lower than the current minimum SSE, the new model is used.
       */
      double last_successful_epoch = 0;
      RBMparameterset trainedparams = new RBMparameterset(params);
      for (int epoch = 0; epoch < max_epochs || !set.atEndOfRandomSets(); epoch++) {

         // If training did not improve SSE for number of epochs, it reverts to the last known best settings
         if (epoch - last_successful_epoch == model_improve_freedom) {
            trainedparams = new RBMparameterset(params);
            last_successful_epoch = epoch;
         }

         // fill the missing input with the expected values to reduce noise
         fixMissing(set, trainedparams);

         // traverse the training set in random order
         if (epoch % 2 == 0) {
            contrastive_divergence(set, trainedparams, learning_rate);
         } else {
            RBMparameterset trainedparams2 = new RBMparameterset( trainedparams );
            awake_divergence(set, trainedparams2, learning_rate);
            if (trainedparams2.sse < trainedparams.sse)
               trainedparams = trainedparams2;
            else
               log.info("awake %f %f", trainedparams2.sse, params.sse);
         }
         log.info("%d %f %f", epoch, trainedparams.sse, params.sse);
         if (trainedparams.sse < params.sse) { // switch if the new parameter set is better than the current
            params = trainedparams;
            trainedparams = new RBMparameterset(params);
            last_successful_epoch = epoch;
            //print(set, params);
            //log.printf("epoch %d minsse %f", epoch, params.sse);
         }
      }
      return params.sse;
   }

   public void fixMissing(RBMtrainingset set, RBMparameterset params) {
      for (Sample sample : set) {
         if (sample.containsmissing) {
            estimate_hidden(sample.visible, params, p_h1, state_h1);
            for (int visible_node = 0; visible_node < num_v; visible_node++) {
               if (sample.missing[visible_node]) {
                  sample.visible.value[0][visible_node] = estimate_visible_singlenode(state_h1, params, visible_node);
               }
            }
         }
      }
   }

   // train one training sample
   public void contrastive_divergence(RBMtrainingset set, RBMparameterset params, double learning_rate) {
      for (Sample sample : set.randomIterable()) {
         estimate_hidden(sample.visible, params, p_h1, state_h1);
         estimate_visible(state_h1, params, p_v2);
         estimate_hidden(p_v2, params, p_h2, state_h2);
         updater.store_weight_updates(sample);
         updater.update_weights(params, 1.0/num_v);
      }
      compute_sse(set, params);
   }

   public void awake_divergence(RBMtrainingset set, RBMparameterset params, double learning_rate) {
      double v2[][] = this.stateResults(params);
      for (Sample sample : set.randomIterable()) {
         estimate_activation_probability(sample.visible, params, p_h1);
         double pstate[] = stateLikelihood(p_h1.value[0]);
         p_v2.value[0] = interpolateV(pstate, v2);
         estimate_activation_probability(p_v2, params, p_h2);
         updater.store_weight_updates(sample);
      }
      updater.update_weights(params, 1.0/ num_v);
      compute_sse(set, params);
   }

   // estimate the activation probability and state of the hidden nodes
   public void estimate_activation_probability(Matrix input_visible, RBMparameterset params, Matrix result_hidden) {
      result_hidden.isDot(input_visible, params.weights_vh);                               // result_hidden = input_visible * weights
      convert_energy_to_act_prob(result_hidden, params.bias_hidden, result_hidden);        // result_hidden = sigmoid( bias + result_hidden )
   }

   public void estimate_hidden(Matrix input_visible, RBMparameterset params, Matrix result_hidden, Matrix state_hidden) {
      estimate_activation_probability(input_visible, params, result_hidden);
      predict_state(result_hidden, state_hidden);                                          // state_hidden = predict_state( result_hidden )
   }

   // estimate the visible nodes based on the state of the hidden nodes
   public void estimate_visible(Matrix state_h, RBMparameterset params, Matrix result_visible) {
      result_visible.isDot(state_h, params.weights_vh.transpose());            // result_visible = state_h * weights
      result_visible.isPlus(params.bias_visible, result_visible);              // result_visible += bias_visible
   }

   // estimate the visible resuolt for a single node
   public double estimate_visible_singlenode(Matrix state_h, RBMparameterset params, int visible_node) {
      double result = params.bias_visible.value[0][visible_node];
      double state_hidden[] = state_h.getRow(0);
      for (int i = 0; i < num_h; i++) {
         result += params.weights_vh.value[visible_node][i] * state_hidden[ i];
      }
      return result;
   }

   public double[][] stateResults(RBMparameterset params) {
      double v2[][] = new double[(int) Math.pow(2, num_h)][num_v];
      for (int i = (int) Math.pow(2, num_h) - 1; i >= 0; i--) {
         for (int j = 0; j < num_h; j++) {
            this.state_h1.value[0][j] = ((i & (1 << j)) > 0) ? 1 : 0;
         }
         estimate_visible(state_h1, params, p_vtemp);
         System.arraycopy(p_vtemp.value[0], 0, v2[i], 0, num_v);
      }
      return v2;
   }

   public double[] stateLikelihood(double p_h[]) {
      double p[] = new double[(int) Math.pow(2, num_h)];
      for (int possiblestate = p.length - 1; possiblestate >= 0; possiblestate--) {
         p[possiblestate] = 1;
         for (int j = 0; j < num_h; j++) {
            if ((possiblestate & (1 << j)) > 0) {
               p[possiblestate] *= p_h[j];
            } else {
               p[possiblestate] *= (1 - p_h[j]);
            }
         }
      }
      return p;
   }

   public double[] interpolateV(double pstate[], double v2[][]) {
      double reconstructed_visible[] = new double[num_v];
      for (int state = 0; state < pstate.length; state++) {
         // interpolate mle of reconstructed visible nodes using statelikelihood and stateresults 
         for (int node = 0; node < num_v; node++) {
            reconstructed_visible[node] += pstate[state] * v2[state][node];
         }
      }
      return reconstructed_visible;
   }

   /**
    * This gives a maximum likelihood for the reconstruction given the visible
    * input and the learned weights. The model is seeded with the visible input,
    * generating activation probabilities for the hidden nodes. For all possible
    * states of the hidden layer the likelihood is then computed using these
    * activation probabilities. The visible reconstructions the hidden layer
    * states are then interpolated using the likelihood of the hidden state as a
    * weight.
    * <p/>
    * @param visible input values for the visible layer
    * @param params learned weights and biases for the model
    * @param p_v2 contains the maximum likelihood of the reconstructed visible
    * layer
    */
   public void estimate_visible_likelihood(Matrix visible, RBMparameterset params, Matrix p_v2) {
      double v2[][] = this.stateResults(params);
      estimate_activation_probability(visible, params, p_h1); // estimate the activation probailities
      double pstate[] = this.stateLikelihood(p_h1.value[0]);
      p_v2.value[0] = interpolateV(pstate, v2);
   }

   public void compute_sse(RBMtrainingset set, RBMparameterset params) {
      double v2[][] = stateResults(params);
      params.sse = 0;
      for (Sample sample : set) {
         estimate_activation_probability(sample.visible, params, p_h1);
         double pstate[] = stateLikelihood(p_h1.value[0]);
         double reconstructed_visible[] = interpolateV(pstate, v2);
         params.sse += sse(sample, reconstructed_visible);
      }
   }

   public double sse(Sample sample, double reconstructed_visible[]) {
      double sse = 0;
      for (int node = 0; node < num_v; node++) {
         if (!sample.missing[node]) {
            sse += Math.pow(sample.visible.value[0][node] - reconstructed_visible[node], 2);
         }
      }
      return sse;
   }

   public void convert_energy_to_act_prob(Matrix bias, Matrix activation_energy, Matrix activation_probability) {
      double b[] = bias.getRow(0);
      double in[] = activation_energy.getRow(0);
      double out[] = activation_probability.getRow(0);
      for (int i = 0; i < b.length; i++) {
         out[i] = activation_probability(b[i] + in[i]);
      }
   }

   /**
    * @param activation_energy
    * @return the probability the node is activated, following a sigmoid
    * function
    */
   public double activation_probability(double activation_energy) {
      return 1.0 / (1.0 + Math.pow(Math.E, -activation_energy));
   }

   /**
    * @param activation_probability the probability a node must be turned on
    * @return 1 or 0 depending on a pulled random number and p.
    */
   public int predict_state(double activation_probability) {
      return (RandomTools.getDouble() < activation_probability) ? 1 : 0;
   }

   public void predict_state(Matrix activation_probability, Matrix output_state) {
      double p[] = activation_probability.getRow(0);
      double state[] = output_state.getRow(0);
      for (int i = 0; i < p.length; i++) {
         state[i] = predict_state(p[i]);
      }
   }

   public void print(RBMtrainingset set, RBMparameterset params) {
      for (Sample sample : set) {
         estimate_hidden(sample.visible, params, p_h1, state_h1);
         estimate_visible_likelihood(sample.visible, params, p_v2);
         log.info("values %s", ArrayTools.toString(sample.visible.getRow(0)));
         log.info("approx %s", ArrayTools.toString(p_v2.getRow(0)));
      }
   }
}
