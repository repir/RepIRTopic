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
public class RBMupdater_orig extends RBMupdater {

   public static Log log = new Log(RBMupdater_orig.class);
   Matrix vec1;
   Matrix vec2;
   
   public RBMupdater_orig(RBM rbm) {
      super( rbm );
      vec1 = new Matrix( num_v, num_h );
      vec2 = new Matrix( num_v, num_h );
   }

   /**
    * Implementation of the original update rules presented by Hinton:
    * gradient = v1*h1 - v2*h2
    * @param sample The training sample that was used to generate the hidden layer
    * and the second generation of the visible and hidden layer.
    */
   public Matrix weight_gradient(Sample sample) {
      vec1.isDot(sample.visible.transpose(), rbm.p_h1);
      vec2.isDot(rbm.p_v2.transpose(), rbm.p_h2);
      vec1.isMinus(vec1, vec2);
      return vec1;
   }
}
