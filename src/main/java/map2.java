import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class map2 extends Mapper<Object, Text, Text, Text> {
    private final double hongkongLat = 22.17;
    private final double hongkongLng = 114.09;
    private static final double EARTH_RADIUS = 6378.137;

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] input = value.toString().split(",");
        String isin = input[5];
        //if the typhoon had affected Hong Kong then pass it to the reducer.
        if (isin.equals("true"))
            context.write(new Text(input[0]), value);
    }
}