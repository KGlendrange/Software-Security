package inf226.util;

public final class Triple<A,B,C> {
   public final A first;
   public final B second;
   public final C third;
   
   public Triple(A first, B second, C third) {
     this.first = first;
     this.second = second;
     this.third = third;
   }

   public static<U,V,Z> Triple <U,V,Z> triple(U first, V second, Z third) {
     return new Triple<U,V,Z>(first, second, third);
   }

   @Override
   public final boolean equals(Object other) {
    if (other == null)
        return false;
    if (getClass() != other.getClass())
        return false;
    @SuppressWarnings("unchecked")
    final Triple<Object,Object,Object> triple_other = (Triple<Object,Object,Object>) other;
    return this.first.equals(triple_other.first)
        && this.second.equals(triple_other.second)
        && this.third.equals(triple_other.third);

  
   }
}
