package it.unipi.hadoop;

import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Represents a point with coordinates, implementing WritableComparable.
 */
public class Point implements WritableComparable<Object> {

    /* Coordinates of the point */
    private ArrayList<Double> coordinates;

    /**
     * Default Constructor. Initializes only the coordinate vector.
     */
    public Point() {
        this.coordinates = new ArrayList<>();
    }

    /**
     * Constructor to initialize a Point object using a string containing coordinates separated by commas.
     * @param value String representing a point.
     */
    public Point(String value) {
        this.coordinates = new ArrayList<>();

        try {
            String[] indicesAndValues = value.split(",");

            for (int i = 0; i < indicesAndValues.length; i++) {
                double coordinate = Double.parseDouble(indicesAndValues[i]);
                coordinates.add(coordinate);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Error converting floating-point value.", e);
        }
    }

    /**
     * Constructor to initialize a Point object with a specific number of coordinates, all set to 0.0.
     * @param d Number of coordinates.
     */
    public Point(int d) {
        this();
        coordinates.addAll(Collections.nCopies(d, 0.0));
    }

    /**
     * Set a new list of coordinates for the Point object.
     * @param coordinates New coordinates.
     */
    public void setCoordinates(ArrayList<Double> coordinates) {
        this.coordinates = coordinates;
    }

    /**
     * Get the coordinates of the current point.
     * @return Coordinates of the point.
     */
    public ArrayList<Double> getCoordinates() {
        return coordinates;
    }

    /**
     * Computes the Euclidean distance between two points with optional coordinate weights.
     * @param p2 Another Point.
     * @return Euclidean distance between the points.
     */
    public double computeDistance(Point p2) {
        if (this.coordinates.size() != p2.getCoordinates().size()) {
            throw new IllegalArgumentException("Point dimensions do not match.");
        }

        double sum = 0;
        ArrayList<Double> p2Coordinates = p2.getCoordinates();

        for (int i = 0; i < this.coordinates.size(); i++) {
            double diff = this.coordinates.get(i) - p2Coordinates.get(i);
            double weightedDiff = Math.pow(diff, 2) * (i + 1); // Weights based on the position of the coordinate
            sum += weightedDiff;
        }

        return Math.sqrt(sum);
    }

    /**
     * Sums the coordinates of the current point with the coordinates of another point.
     * @param p2Point Another Point.
     */
    public void sumPoints(Point p2Point) {
        if (this.coordinates.size() != p2Point.getCoordinates().size()) {
            throw new IllegalArgumentException("Point dimensions do not match.");
        }

        for (int i = 0; i < this.coordinates.size(); i++) {
            double sum = this.coordinates.get(i) + p2Point.getCoordinates().get(i);
            this.coordinates.set(i, sum);
        }
    }

    /**
     * Divides every coordinate of the Point object by an integer.
     * @param num Divisor.
     */
    public void divideEveryCoordinate(int num) {
        if (num == 0) {
            throw new ArithmeticException("Cannot divide by zero.");
        }

        if (num < 0) {
            throw new IllegalArgumentException("The divisor must be a positive number.");
        }

        for (int i = 0; i < coordinates.size(); i++) {
            double currentCoordinate = coordinates.get(i);

            // Overflow check to avoid unexpected results in case of division with very large numbers
            if (currentCoordinate == Double.POSITIVE_INFINITY || currentCoordinate == Double.NEGATIVE_INFINITY) {
                throw new ArithmeticException("Overflow during division.");
            }

            double result = currentCoordinate / num;

            // NaN (Not a Number) check to handle potential error situations
            if (Double.isNaN(result)) {
                throw new ArithmeticException("Division result is not a valid number (NaN).");
            }

            coordinates.set(i, result);
        }
    }

    /**
     * Converts the Point object to a string representation.
     * @return String representation of the Point.
     */
    public String toString() {
        return String.join(",", coordinates.stream()
                .map(Object::toString)
                .toArray(String[]::new));
    }

    /**
     * Writes the attributes of the Point object to DataOutput.
     * @param out DataOutput object for writing.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        try {
            out.writeInt(coordinates.size());

            for (Double c : coordinates) {
                if (c.isNaN() || c.isInfinite()) {
                    throw new IllegalStateException("Cannot write NaN or Infinite coordinates.");
                }
                out.writeDouble(c);
            }
        } catch (IOException e) {
            throw new IOException("Error during data writing.", e);
        }
    }

    /**
     * Reads the attributes of the Point object from DataInput.
     * @param in DataInput object for reading.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            int size = in.readInt();

            if (size < 0) {
                throw new IOException("Negative coordinate size during reading.");
            }

            coordinates = new ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                double coordinate = in.readDouble();

                if (Double.isNaN(coordinate) || Double.isInfinite(coordinate)) {
                    throw new IllegalStateException("Read NaN or Infinite coordinates.");
                }

                coordinates.add(coordinate);
            }
        } catch (IOException e) {
            throw new IOException("Error during data reading.", e);
        }
    }

    /**
     * Compares two Point objects to determine their relative order.
     * @param o Another Object to compare.
     * @return Comparison result.
     */
    @Override
    public int compareTo(Object o) {
        if (o == null || getClass() != o.getClass()) {
            throw new ClassCastException("Cannot compare objects of different classes or with null.");
        }

        ArrayList<Double> thisCoordinates = this.getCoordinates();
        ArrayList<Double> thatCoordinates = ((Point) o).getCoordinates();

        if (thisCoordinates.size() != thatCoordinates.size()) {
            throw new IllegalArgumentException("Cannot compare points with different dimensions.");
        }

        for (int i = 0; i < thisCoordinates.size(); i++) {
            int comparison = Double.compare(thisCoordinates.get(i), thatCoordinates.get(i));

            if (comparison != 0) {
                return comparison;
            }
        }

        return 0;
    }
}
