import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class reduce1 extends Reducer<Text, Text, Text, Text> {

    private final double hongkongLat = 22.17;
    private final double hongkongLng = 114.09;
    private static final double EARTH_RADIUS = 6378.137;
    private static double rad(double d)
    {
        return d * Math.PI / 180.0;
    }

    //methods used to calculate distance between two locations
    public static double GetDistance(double lat1, double lng1, double lat2, double lng2)
    {
        double radLat1 = rad(lat1);
        double radLat2 = rad(lat2);
        double a = radLat1 - radLat2;
        double b = rad(lng1) - rad(lng2);
        double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) +
                Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)));
        s = s * EARTH_RADIUS;
        s = Math.round(s * 10000) / 10000;
        return s;
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String isTrue = "false";

        //cache the values(reduce can only iterate once)
        ArrayList<String> cache = new ArrayList<String>();
        for (Text e: values){
            cache.add(e.toString());
            String[] input = e.toString().split(",");
            String[] coord = input[2].trim().split("\\s+");
            double tyLat = Double.parseDouble(coord[0]);
            double tyLng = Double.parseDouble(coord[2]);
            double distance = GetDistance(tyLat, tyLng, hongkongLat,hongkongLng );
            // if the distance smaller than 800 KM, then the typhoon had affected Hong Kong.
            if (distance < 800) {
                isTrue = "true";
            }
        }

        for (String e: cache){
            context.write(null, new Text(e+","+isTrue));
        }

    }

}