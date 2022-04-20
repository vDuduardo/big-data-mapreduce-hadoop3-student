package TDE01.Writables;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class MaxMinMeanWritable implements WritableComparable<MaxMinMeanWritable>{
    /**
     * Todo writable precisa ser um Java BEAN!
     * 1 - Construtor vazio
     * 2 - Gets e sets
     * 3 - Comparação entre objetos
     * 4 - Atributos privados
     */

    private double max;
    private double min;
    private double n;
    private double sum;


    public MaxMinMeanWritable(){

    }

    public MaxMinMeanWritable(double max, double min, double n, double sum) {
        this.max = max;
        this.min = min;
        this.n = n;
        this.sum = sum;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public void setN(double n) {
        this.n = n;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public double getMax() {
        return max;
    }

    public double getMin() {
        return min;
    }

    public double getN() {
        return n;
    }

    public double getSum() {
        return sum;
    }

    public int compareTo(MaxMinMeanWritable o){
        if(this.hashCode() > o.hashCode()){
            return +1;
        }else if(this.hashCode() < o.hashCode()){
            return -1;
        }
        return 0;
    }

    public void write(DataOutput dataOutput) throws IOException{
        dataOutput.writeDouble(max);
        dataOutput.writeDouble(min);
        dataOutput.writeDouble(n);
        dataOutput.writeDouble(sum);
    }

    public void readFields(DataInput dataInput) throws IOException {
        max = dataInput.readDouble();
        min = dataInput.readDouble();
        n = dataInput.readDouble();
        sum = dataInput.readDouble();
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MaxMinMeanWritable that = (MaxMinMeanWritable) o;
        return max == that.max && min == that.min && sum == that.sum && n == that.n;
    }

    @Override
    public String toString() {
        return String.format("Maximum: %s - Minimum: %s - Average: %s", max, min, sum/n);
    }
}
