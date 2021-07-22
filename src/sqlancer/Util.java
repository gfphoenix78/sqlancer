package sqlancer;

public class Util {
    // if cond is true, filter bad items
    public static <T> T []filter(T []array, boolean cond, T ...bad) {
        if (!cond)
            return array;
        int i, k;
        T []copy = (T[]) new Object[array.length];
        for (i = 0, k = 0; i < array.length; i++) {
            boolean good = true;
            for (T x : bad) {
                if (array[i] == x) {
                    good = false;
                    break;
                }
            }
            if (good) {
                copy[k] = array[i];
                k++;
            }
        }
        T []newArray = (T[]) new Object[k];
        System.arraycopy(copy, 0, newArray, 0, k);
        return newArray;
    }
}
