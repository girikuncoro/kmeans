import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class UpdateJobRunner
{
    /**
     * Create a map-reduce job to update the current centroids.
     * @param jobId Some arbitrary number so that Hadoop can create a directory "<outputDirectory>/<jobname>_<jobId>"
     *        for storage of intermediate files.  In other words, just pass in a unique value for this
     *        parameter.
     * @param The input directory specified by the user upon executing KMeans, in which the points
     *        to find the KMeans point files are located.
     * @param The output directory for which to write job results, specified by user.
     * @precondition The global centroids variable has been set.
     */
    public static Job createUpdateJob(int jobId, String inputDirectory, String outputDirectory)
        throws IOException
    {
    	System.out.println("============Creating job configuration");
    	// Create new job configuration
    	Job job = new Job(new Configuration(), "job"+Integer.toString(jobId));
    	job.setJarByClass(KMeans.class);
    	
    	System.out.println("=============Setting mapper class");
    	job.setMapperClass(PointToClusterMapper.class);
    	System.out.println("=============Setting mapoutput key class");
    	job.setMapOutputKeyClass(IntWritable.class);
    	job.setMapOutputValueClass(Point.class);
    	job.setReducerClass(ClusterToPointReducer.class);
    	job.setOutputKeyClass(IntWritable.class);
    	job.setOutputValueClass(Point.class);
    	
    	FileInputFormat.addInputPath(job, new Path(inputDirectory));
    	FileOutputFormat.setOutputPath(job, new Path(outputDirectory + "/job" + Integer.toString(jobId)));
    	job.setInputFormatClass(KeyValueTextInputFormat.class);
    	
    	System.out.println("=============Return job");
    	
        return job;
    }

    /**
     * Run the jobs until the centroids stop changing.
     * Let C_old and C_new be the set of old and new centroids respectively.
     * We consider C_new to be unchanged from C_old if for every centroid, c, in 
     * C_new, the L2-distance to the centroid c' in c_old is less than [epsilon].
     *
     * Note that you may retrieve publically accessible variables from other classes
     * by prepending the name of the class to the variable (e.g. KMeans.one).
     *
     * @param maxIterations   The maximum number of updates we should execute before
     *                        we stop the program.  You may assume maxIterations is positive.
     * @param inputDirectory  The path to the directory from which to read the files of Points
     * @param outputDirectory The path to the directory to which to put Hadoop output files
     * @return The number of iterations that were executed.
     */
	public static ArrayList<Point> oldCentroids = new ArrayList<Point>();
	
    public static int runUpdateJobs(int maxIterations, String inputDirectory,
        String outputDirectory) {
    	int iteration = 0;
    	boolean isChanged = true;
    	Job[] jobs = new Job[maxIterations];
    	
    	for(int i=0; i<KMeans.centroids.size(); i++) {
    		oldCentroids.add(new Point(KMeans.centroids.get(i)));
    	}
    	
    	System.out.println("=============Run iteration");
    	// Iteratively runs the kmeans map reduce jobs for maxIterations
    	// or until the centroids don't change anymore
    	while(iteration <= maxIterations && isChanged) {
    		for(int i=0; i<KMeans.centroids.size(); i++) {
        		oldCentroids.set(i, new Point(KMeans.centroids.get(i)));
        	}
    		
    		// Create map reduce jobs
    		try {	
    			jobs[iteration] = createUpdateJob(iteration, inputDirectory, outputDirectory);
    			System.out.println("=========Finished create update job");
    			jobs[iteration].waitForCompletion(true);
    			System.out.println("=========Finished wait completion");
    		} catch(Exception e) {
    			System.out.println("Create map reduce jobs failed");
    		}
    		isChanged = changeStatus();
    		iteration++;
    	}
    	
    	return iteration;
    }
    
    /**
     * Check if new centroids are changed, see if converged 
     * @return boolean
     */
    public static boolean changeStatus() {
    	for(int i=0; i<oldCentroids.size(); i++) {
    		if(KMeans.centroids.get(i).compareTo(oldCentroids.get(i)) != 0) {
    			return true;
    		}
    	}
    	return false;
    }
}
