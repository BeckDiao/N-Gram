import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class SearchEngine {

	public static void main(String[] args) throws Exception {
		
		if (args.length < 3) { //0: input, 1: output, 2: stopwords
			throw new Exception("Usage: <input dir> <output dir> <stopwords path>");
		}
		
		Configuration conf = new Configuration();
		conf.set("filePath", args[2]); // key-value, 其实就是用一个key 来给后面的class调用，实际用的是value
		Job job = Job.getInstance(conf);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.setNumReduceTasks(3);
		job.setJarByClass(SearchEngine.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}

}
