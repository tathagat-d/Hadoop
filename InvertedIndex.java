import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Hashtable;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex 
{
    public static class MyMapper
	    extends Mapper<LongWritable, Text, Text, Text>
    {
	private Text word = new Text();
	private Text KEY = new Text();

	/* key is the documentID provided by KeyValueTextInputFormat. */
	public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException
	    {
		String line = value.toString();
		line = line.toLowerCase();
		StringTokenizer itr = new StringTokenizer(line);
		if (itr.hasMoreTokens())
		    KEY.set(itr.nextToken());

		while (itr.hasMoreTokens()) 
		{
		    word.set(itr.nextToken());
		    context.write(word, KEY);
		}
	    }
    }


    public static class MyReducer
	    extends Reducer<Text, Text, Text, Text>
    {

	public void reduce(Text key, Iterable<Text> values, Context con)
	    throws IOException, InterruptedException
	    {
		Hashtable<String, Integer> table = new Hashtable<String, Integer>();
		Text extendedResult = new Text();
		StringBuilder result = new StringBuilder();
		String temp = new String();
		int count = 0;
		for (Text  value : values) {
		    String pankaj = value.toString();
		    if (table.containsKey(pankaj))
			table.put(pankaj, table.get(pankaj) + 1);
		    else
			table.put(pankaj, 1);
		}
		
		/* Converting the hastable into string */
		Set<String> docIDs = table.keySet();
		for (String docID : docIDs) {
		    temp  = docID + ":" + Integer.toString(table.get(docID)) + " ";
		    result.append(temp);
		}

		extendedResult.set(result.toString());
		con.write(key, extendedResult);
	    }
    }

    public static void main (String [] args) throws Exception
    {
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "Inverted Indexing");

	job.setJarByClass(InvertedIndex.class);
	job.setMapperClass(MyMapper.class);
	job.setReducerClass(MyReducer.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
