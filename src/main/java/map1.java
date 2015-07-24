import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class map1 extends Mapper<Object, Text, Text, Text>{

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] input = value.toString().split(",");
        context.write(new Text(input[0]), value);
    }
}
