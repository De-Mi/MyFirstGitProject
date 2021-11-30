import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Trigram implements WritableComparable<Trigram> {
    
    private Text first;
    private Text second;
    private Text third;
    private Text fourth;
    private Text fifth;
    
    public Trigram(Text first, Text second, Text third, Text fourth, Text fifth) {
        /* 
        Constructor takes three Text objects
        */
        set(first,second,third, fourth, fifth);
    }
    
    public Trigram() {
        /* 
        Instantiation without params creates a trigram with
        three empty Text objects 
        */        
        set(new Text(), new Text(), new Text(), new Text(), new Text());
    }
    
    public void set(Text one, Text two, Text three, Text fourth, Text fifth) {
        /* 
        Sets the three Trigram attributes
        */
        this.first = one;
        this.second = two;
        this.third = three;
        this.fourth = fourth;
        this.fifth = fifth;
    }
    
    public Text getFirst() {
        // getter
        return first;
    }
    
    public Text getSecond() {
        // getter
        return second;
    }
    
    public Text getThird() {
        // getter
        return third;
    }
    
    public Text getFourth() {
        // getter
        return fourth;
    }
    
    public Text getFifth() {
        // getter
        return fifth;
    }
    
    @Override
    public String toString() {
        /* 
        Convert a Trigram instance to a string of comma 
        separated values: "first, second, third"
        */
        return first.toString() + " " +
                second.toString() + " " +
                third.toString() +" "+ fourth.toString() +" " +fifth.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        /*
        Method to write out Trigram objects. 
        Notice that hadoop's Text class makes it easy via it's 
        write() method!
        */
        first.write(out);
        second.write(out);
        third.write(out);
        fourth.write(out);
        fifth.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        /*
        Method to read in values.
        
        Once again, Hadoop makes it easy to read in values via the 
        Text class's readFields() method. 
        */
        first.readFields(in);
        second.readFields(in);
        third.readFields(in);
        fourth.readFields(in);
        fifth.readFields(in);
    }
    
    
    @Override
    public int compareTo(Trigram other) {
        /*
        Method to perform a bytewise comparison of
        two objects. Method returns a non-zero
        integer value when two objects are not equal. 
        */
        
        int compared = first.compareTo(other.first);
        
        if (compared != 0) {
            return compared;
        }
        
        compared = second.compareTo(other.second);
        
        if (compared != 0) {
            return compared;
        }
        
        compared = third.compareTo(other.third);
        
        if (compared != 0) {
            return compared;
        }
        
        compared = fourth.compareTo(other.fourth);
        if (compared != 0) {
            return compared;
        }
        
        return fifth.compareTo(other.fifth);

    }
    
 //   @Override
    public boolean equals(Object other) {
        /* 
        Return true if two instance of Trigram have the same contents. This
        helps us conform to Java's Object interface, however Hadoop MapReduce does
        not use the equals method in performing MR jobs. 
        */
        
        if (other instanceof Trigram) { // short circuit if not a Trigram
            // since we know other is a Trigram, explicitly cast it as such
            Trigram trigram = (Trigram) other;

            return first.equals(trigram.first) && second.equals(trigram.second)
                && third.equals(trigram.third) && fourth.equals(trigram.fifth) && fifth.equals(trigram.fifth);
        }
        return false;
        
        
        
    }
    
    @Override
    public int hashCode() {
        /*
        The hashCode() implementation should be stable - meaning it can be
        computed consistently across various JVMs. 
        
        In this case, multiply first.hashCode() by a prime and add the hashCodes
        of second and third. This should sufficient to ensure the value is 
        unique for a Trigram with a particular set of attributes.
        
        */
        
        return first.hashCode()*163 + second.hashCode() + third.hashCode() + fourth.hashCode() + fifth.hashCode();
    }

}