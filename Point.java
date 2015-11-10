import java.io.*; // DataInput/DataOuput
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.io.*; // Writable

/**
 * A Point is some ordered list of floats.
 * 
 * A Point implements WritableComparable so that Hadoop can serialize
 * and send Point objects across machines.
 *
 * NOTE: This implementation is NOT complete.  As mentioned above,  you need
 * to implement WritableComparable at minimum.  Modify this class as you see fit.
 */
public class Point implements WritableComparable<Point> {
	ArrayList<Float> attrs = new ArrayList<Float>();
	int dimension = 0;
	
    /**
     * Construct a Point with the given dimensions [dim]. The coordinates should all be 0.
     * For example:
     * Constructing a Point(2) should create a point (x_0 = 0, x_1 = 0)
     */
    public Point(int dim)
    {
    	dimension = dim;
    	for(int i=0; i<dim; i++) {
    		attrs.add(new Float(.0));
    	}
    }

    /**
     * Construct a point from a properly formatted string (i.e. line from a test file)
     * @param str A string with coordinates that are space-delimited.
     * For example: 
     * Given the formatted string str="1 3 4 5"
     * Produce a Point {x_0 = 1, x_1 = 3, x_2 = 4, x_3 = 5}
     */
    public Point(String str)
    {
        String[] nums = str.split(" ");
        dimension = nums.length;
        for(String n:nums) {
        	attrs.add(Float.parseFloat(n));
        }
    }

    /**
     * Copy constructor
     */
    public Point(Point other)
    {
    	this.dimension = other.getDimension();
    	for(int i=0; i<dimension; i++) {
    		attrs.add(other.attrs.get(i));
    	}
    }
	
    /**
     * @return The dimension of the point.  For example, the point [x=0, y=1] has
     * a dimension of 2.
     */
    public int getDimension()
    {
        return dimension;
    }

    /**
     * Converts a point to a string.  Note that this must be formatted EXACTLY
     * for the autograder to be able to read your answer.
     * Example:
     * Given a point with coordinates {x=1, y=1, z=3}
     * Return the string "1 1 3"
     */
    public String toString()
    {
    	String res = "";
    	for(int i=0; i<dimension-1; i++) {
    		res += attrs.get(i) + " ";
    	}
    	return res + attrs.get(dimension-1);
    }

	/**
	 * Serialize the fields of this object to out
	 * @param out, DataOutput to serialize this object into
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(dimension);
		for(int i=0; i<dimension; i++) {
			out.writeFloat(attrs.get(i));
		}
	}
	
    /**
     * Deserialize the fields of this object from in.
     * For efficiency, implementations should attempt to 
     * reuse storage in the existing object where possible
     * @param in, DataInput to deseriablize this object from
     */
	@Override
	public void readFields(DataInput in) throws IOException {
		dimension = in.readInt();
		for(int i=0; i<dimension; i++) {
			attrs.set(i, in.readFloat());
		}
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int res = 1;
		int tmp = 0;
		res = prime * res + dimension;
		for(Float a : attrs) {
			tmp += a;
		}
		return res * prime + tmp;
	}
	
    /**
     * One of the WritableComparable methods you need to implement.
     * See the Hadoop documentation for more details.
     * You should order the points "lexicographically" in the order of the coordinates.
     * Comparing two points of different dimensions results in undefined behavior.
     */
	@Override
	public int compareTo(Point o) {
		// Undefined behavior for different dimensions
		if(this.dimension != o.dimension) {
			System.err.println("Different dimensions, undefined behavior");
			System.exit(1);
		}
		
		double offset = 0.000001;
		// Order lexicographically
		for(int i=0; i<dimension; i++) {
			double difference = this.attrs.get(i) - o.attrs.get(i);
			if(difference > offset) {
				return 1;
			} else if(difference < offset) {
				return -1;
			}
		}
		return 0;
	}
	
    /**
     * @return The L2 distance between two points.
     */
    public static final float distance(Point x, Point y)
    {
    	// Undefined behavior for different dimensions
    	if(x.dimension != y.dimension) {
    		System.err.println("Different dimensions, undefined behavior");
    		System.exit(1);
    	}
    	
    	// Calculate Euclidean distance
    	double agg = 0.0; 
    	
    	for(int i=0; i<x.dimension; i++) {
    		double diff = Math.abs(x.attrs.get(i) - y.attrs.get(i));
    		agg += diff*diff;
    	}
    	
        return (float)Math.sqrt(agg);
    }

    /**
     * @return A new point equal to [x]+[y]
     */
    public static final Point addPoints(Point x, Point y)
    {
    	// Use longer dimensions if differ
    	int dim;
    	if(x.dimension != y.dimension) {
    		dim = x.dimension > y.dimension ? x.dimension : y.dimension;
    	} else {
    		dim = x.dimension;
    	}
    	
    	// Aggregate points
    	Point res = new Point(dim);
    	for(int i=0; i<dim; i++) {
    		res.attrs.set(i, new Float(x.attrs.get(i).floatValue() + y.attrs.get(i).floatValue()));
    	}
    	return res;
    }

    /**
     * @return A new point equal to [c][x]
     */
    public static final Point multiplyScalar(Point x, float c)
    {
    	Point res = new Point(x.dimension);
    	for(int i=0; i<res.dimension; i++) {
    		res.attrs.set(i, new Float(x.attrs.get(i).floatValue() * c));
    	}
    	return res;
    }
}
