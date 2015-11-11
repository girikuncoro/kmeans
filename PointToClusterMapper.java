import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * You can modify this class as you see fit.  You may assume that the global
 * centroids have been correctly initialized.
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 */
public class PointToClusterMapper extends Mapper<Text, Text, IntWritable, Point> {
	// Mapper needs to know what the current centroids are
	// Each map tasks works on a single point and assigns it
	// to the current centroid with the least Euclidian distance
	
	// Called once for each key/value pair in the input split
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		Point point = new Point(key.toString());
		Point nearestCent = null;
		float nearestDistance = Float.MAX_VALUE;
		int nearestIdx = 0;
		
		// Get config for the job
		Configuration config = context.getConfiguration();
		
		// Find least Euclidian distance
		for(int i=0; i<KMeans.centroids.size(); i++) {
			Point cent = new Point(KMeans.centroids.get(i));
			float dist = Point.distance(cent, point);
			if(nearestCent == null || dist < nearestDistance) {
				nearestCent = new Point(cent);
				nearestDistance = dist;
				nearestIdx = i;
			}
		}
		
		IntWritable cKey = new IntWritable(nearestIdx);
		context.write(cKey, point);
	}
}
