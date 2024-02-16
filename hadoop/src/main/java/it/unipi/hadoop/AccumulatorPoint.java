package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AccumulatorPoint extends Point {
    private int size;

    /** Default Constructor
     *
     */
    public AccumulatorPoint() {
        super(); // Call the default constructor of the superclass (Point)
    }

    /** Parameterized Constructor
     *
     * @param d
     */
    public AccumulatorPoint(int d) {
        super(d);
        size = 0;
    }

    /** Method to sum the coordinates of another Point to this AccumulatorPoint
     *
     * @param that
     */
    public void sumPoints(Point that) {
        super.sumPoints(that);
        size++;
    }

    /** Method to provide a string representation of the AccumulatorPoint
     *
     * @return
     */
    @Override
    public String toString() {
        return super.toString() + " " + this.size; // Return a string with coordinates and size
    }

    /** Write method for serialization
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(size);
    }

    // Getter method for the size variable
    public int getSize() {
        return size;
    }

    /** Read method for deserialization
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        size = in.readInt();
    }
}
