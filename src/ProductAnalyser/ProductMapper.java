package ProductAnalyser;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;


public class ProductMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Logger logger = Logger.getLogger(ProductMapper.class);
	
    @Override
    public void map(Object key, Text value, Context output) throws IOException,
            InterruptedException {

        //If more than one word is present, split using white space.
        String[] words = value.toString().split("::");
        
        // This first word is the County Name.
        int lineweight=0;
//       logger.info("Mapper:" + ProductAnalyserAppn.arg3);
//        //System.out.println("Mapper:" + ProductAnalyserAppn.arg3);
//        if(ProductAnalyserAppn.arg3.equals("Kids")){
//        	
//        	 logger.info("Mapper:Kids");
//        	 lineweight = Integer.parseInt(words[1].trim()) + Integer.parseInt(words[3].trim()) + Integer.parseInt(words[4].trim());
//        
//        }else if(ProductAnalyserAppn.arg3.equals("Teens")){
//        	
//        	logger.info("Mapper:Teens");
//        	lineweight = Integer.parseInt(words[2].trim()) + Integer.parseInt(words[3].trim()) + Integer.parseInt(words[4].trim());
//        
//        }else{
//        	
//        	logger.info("Mapper:None");
//        	lineweight = Integer.parseInt(words[1].trim()) + Integer.parseInt(words[2].trim()) + Integer.parseInt(words[3].trim()) + Integer.parseInt(words[4].trim());
//
//        }
        
        lineweight = Integer.parseInt(words[1].trim()) + Integer.parseInt(words[2].trim()) + Integer.parseInt(words[3].trim()) ;
        
        IntWritable lineValue = new IntWritable(lineweight);
        
        output.write(new Text(words[0]), lineValue);
    }
	
}
