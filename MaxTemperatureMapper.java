import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
public class MaxTemperatureMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> 
{
    private static final int MISSING = 9999;
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
    {
        String line = value.toString();
        String year = line.substring(0, 4);
        int airTemperature;
        if (line.charAt(4) == '+') 
        {
            airTemperature = Integer.parseInt(line.substring(5, 10));
        } 
        else 
        {
            System.out.println( line );
            airTemperature = Integer.parseInt(line.substring(4, 9));
        }
        System.out.println( "Mapper: " + year + ", " + airTemperature );
        output.collect(new Text(year), new IntWritable(airTemperature));
    }
}
