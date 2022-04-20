package TDE01.Writables;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CommercialTransactionWritable implements WritableComparable<CommercialTransactionWritable>{


    private int n;
    private double soma;

    public CommercialTransactionWritable(){

    }

    public CommercialTransactionWritable (int n, double soma) {
        this.n = n;
        this.soma = soma;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public double getSoma() {
        return soma;
    }

    public void setSoma(double soma) {
        this.soma = soma;
    }

    public int compareTo(CommercialTransactionWritable o){
        if(this.hashCode() > o.hashCode()){
            return +1;
        }else if(this.hashCode() < o.hashCode()){
            return -1;
        }
        return 0;
    }

    //Jogar informacao de um PC para outro ou gravar em disco
    public void write(DataOutput dataOutput) throws IOException{
        dataOutput.writeInt(n); //OUtro PC precisa ler as informações na mesma ordem de escrita
        dataOutput.writeDouble(soma);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        n = dataInput.readInt();
        soma = dataInput.readDouble();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommercialTransactionWritable that = (CommercialTransactionWritable) o;
        return n == that.n && Double.compare(that.soma, soma) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(n, soma);
    }
}

