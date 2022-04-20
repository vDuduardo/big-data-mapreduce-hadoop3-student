package TDE01.Writables;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MostCommercializedCommodityWritable implements WritableComparable<MostCommercializedCommodityWritable> {


    private double quantity;
    private String commodity;

    public MostCommercializedCommodityWritable(double quantity, String commodity) {
        this.quantity = quantity;
        this.commodity = commodity;
    }

    public MostCommercializedCommodityWritable() {}

    public double getQuantity() { return quantity; }
    public String getCommodity() { return commodity; }
    public void setQuantity(double quantity) { this.quantity = quantity; }
    public void setCommodity(String commodity) { this.commodity = commodity; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MostCommercializedCommodityWritable that = (MostCommercializedCommodityWritable) o;
        return quantity == that.quantity &&
                commodity.equals(that.commodity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(quantity, commodity);
    }

    @Override
    public int compareTo(MostCommercializedCommodityWritable o) {
        if(this.hashCode() > o.hashCode()) {
            return +1;
        } else if (this.hashCode() < o.hashCode()) {
            return -1;
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(this.quantity);
        dataOutput.writeUTF(this.commodity);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.quantity = dataInput.readDouble();
        this.commodity = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "quantity=" + quantity + ", commodity=" + commodity;
    }
}