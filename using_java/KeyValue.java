public class KeyValue {
    public float minimum;
    public float maximum;
    public float sum;
    public int count;

    public KeyValue() {
        this.minimum = 9999.0f;
        this.maximum = 0.0f;
        this.sum = 0.0f;
        this.count = 0;
    }

    // Parameterized constructor
    public KeyValue(float minm, float sums, float maxm, int c) {
        this.minimum = minm;
        this.maximum = maxm;
        this.sum = sums;
        this.count = c;
    }

    @Override
    public String toString(){
        return "" + minimum + "," + sum/count + "," + maximum + "," + count;
    }
}