package util;
import io.github.repir.tools.Lib.Log; 

/**
 *
 * @author Jeroen Vuurens
 */
abstract class Parent {
  protected int parentvar = 2;

  public Parent() {
    invokeMethod();
  }

  public abstract void invokeMethod();
  
   public static void main(String[] args) {
      new Child();
   }
}

class Child extends Parent {
  protected int childvar = 1;

  public Child() {
     super();
     childvar = 2;
  }
  
  public void invokeMethod() {
     System.out.println("" + childvar);
  }
}
