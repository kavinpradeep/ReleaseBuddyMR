package ProductAnalyser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ProductAnalyserAppn  extends Configured implements Tool{

	public static String arg3 = ""; 
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
        int res=0;
		try {
			res = ToolRunner.run(new Configuration(), new ProductAnalyserAppn(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			res=1;
		}
        System.exit(res);   
        
	}

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("usage: [input] [output]");
            System.exit(-1);
        }

        Job job = Job.getInstance(new Configuration());
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(ProductMapper.class);
        job.setReducerClass(ProductReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if(args.length>=3){
        	System.out.println("Product Analyser Arguments args3:" + args[2]);
        	ProductAnalyserAppn.arg3=args[2].trim();
        	System.out.println("Product Analyser Arguments ProductAnalyserAppn.args3:" + ProductAnalyserAppn.arg3);
        }
        
        job.setJarByClass(ProductAnalyserAppn.class);

        job.submit();
        return 0;
    }
	
	
}
