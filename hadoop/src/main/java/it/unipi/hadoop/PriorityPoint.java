package it.unipi.hadoop;

import java.util.Objects;

/**
 * Represents a point with coordinates and priority, extending the Point class.
 */
public class PriorityPoint extends Point implements Comparable<Object> {
    private final int priority;

    /**
     * Constructor PriorityPoint(int priority, String value) initializes a PriorityPoint object with a given priority
     * and coordinate values (value).
     * @param priority Priority value.
     * @param value String representing coordinates.
     */
    public PriorityPoint(int priority, String value) {
        super(value);
        this.priority = priority;
    }

    /**
     * Getter for priority.
     * @return Priority value.
     */
    public int getPriority() {
        return priority;
    }

    /**
     * The compareTo method in the PriorityPoint class implements the comparison logic
     * between two PriorityPoint objects to establish the relative order based on priority.
     * @param other Another PriorityPoint to compare.
     * @return Comparison result.
     */
    public int compareTo(PriorityPoint other) {
        // Compare based on priority, higher priority comes first
        return Integer.compare(other.priority, this.priority);
    }

    /**
     * The toString method provides a string representation of the PriorityPoint object,
     * including coordinates and priority.
     * @return String representation of the PriorityPoint.
     */
    public String toString() {
        return super.toString() + " Priority: " + this.priority;
    }

    /**
     * The equals method is an override of the default equals method in the Object class
     * and has been implemented in the PriorityPoint class.
     * @param o Another object to compare.
     * @return True if the objects are equal, false otherwise.
     */
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        PriorityPoint that = (PriorityPoint) o;
        return priority == that.priority;
    }

    /**
     * The hashCode method generates a hash code for the PriorityPoint object,
     * taking into account both coordinates and priority.
     * @return Hash code for the PriorityPoint.
     */
    public int hashCode() {
        return Objects.hash(super.hashCode(), priority);
    }
}
