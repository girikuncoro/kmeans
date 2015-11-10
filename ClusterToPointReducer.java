import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/** 
 * You can modify this class as you see fit, as long as you correctly update the
 * global centroids.
 */
public class ClusterToPointReducer extends Reducer<IntWritable, Point, IntWritable, Point> {
	// Each reducer takes all points assigned to a cluster
	// computes the new centroid (sum the points and divide by number of points in cluster)
	public void reduce(IntWritable key, Iterable<Point> values, Context context) 
			throws IOException, InterruptedException{
		int counter = 0;
		Point newCentroid = new Point(KMeans.centroids.get(0).getDimension());
		
		// Sum points divided by number of points in cluster
		for(Point p : values) {
			counter++;
			newCentroid = Point.addPoints(newCentroid, p);
		}
		newCentroid = Point.multiplyScalar(newCentroid, 1.0f/(float)counter);
		
		// Write key value pair in the context
		context.write(key, newCentroid);
		
		// Update global centroids with newCentroid
		KMeans.centroids.set(key.get(), newCentroid);
	}
}
