import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


public class reduce3 extends Reducer<Text, Text, Text, Text> {
        /*
       group1: start from pacific ocean
       group2: start from the South China Sea and heading east
       group3: start from the South China Sea and heading west
         */

    private final double hongkongLat = 22.17;
    private final double hongkongLng = 114.09;
    private static final double EARTH_RADIUS = 6378.137;
    private final String group1 = "start from pacific ocean";
    private final String group2 = "start from the South China Sea and heading east";
    private final String group3 = "start from the South China Sea and heading west";
    private static double rad(double d)
    {
        return d * Math.PI / 180.0;
    }

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
        ArrayList<String> cache = new ArrayList<String>();
        String groupNum = key.toString().trim();
        double count = 0;
        double speedSum = 0;
        ;
        for (Text e: values){
            String[] input = e.toString().split(",");
            String[] coord = input[3].trim().split("\\s+");
            double tyLat = Double.parseDouble(coord[0]);
            double tyLng = Double.parseDouble(coord[2]);
            double distance = GetDistance(tyLat, tyLng, hongkongLat,hongkongLng );

            // if the distance between the typhoon and Hong Kong is less than 800Km, it will be count into the average strengh
            if (distance < 800) {
                count ++;
                String[] speed = input[4].trim().split("\\s+");
                speedSum += Double.parseDouble(speed[0]);
            }
        }

        String outputKey = "";
        if (groupNum.equals("1"))
            outputKey = "Group 1: " + group1;
        if (groupNum.equals("2"))
            outputKey = "Group 2: " + group2;
        if (groupNum.equals("3"))
            outputKey = "Group 3: " + group3;

        double averageSpeed = speedSum/count;

        context.write (new Text(outputKey), new Text(count + " Average Speed: " + averageSpeed +" km/h"));

    }

}
