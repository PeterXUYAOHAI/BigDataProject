import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by asus on 2015/7/22.
 */
public class reduce2 extends Reducer<Text, Text, Text, Text> {
    /*
    This reduce is to classify the typhoones into three group
     group1: start from pacific ocean
     group2: start from the South China Sea and heading east
     group3: start from the South China Sea and heading west
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> cache = new ArrayList<String>();
        String groupId = "";
        String earlist = "";
        int earlistTime = 999999;
        int latestTime = 0;
        for (Text e : values) {
            cache.add(e.toString());
            String[] input = e.toString().split(",");
            int time = Integer.parseInt(input[1]);
            //find the eaelistTime and latestTime of Typhoon
            if (time < earlistTime)
                earlistTime = time;
            if (time > latestTime)
                latestTime = time;
        }

        for (String e : cache) {
            String[] input1 = e.split(",");
            if (Integer.parseInt(input1[1]) == earlistTime) {
                String[] coordE = input1[2].trim().split("\\s+");
                double tyLatE = Double.parseDouble(coordE[0]);
                double tyLngE = Double.parseDouble(coordE[2]);
                //if the start point of typhoon is west of 123E, then it may be group 2 or group 3
                if (tyLngE < 123) {
                    for (String s : cache) {
                        String[] input2 = s.split(",");
                        if (Integer.parseInt(input2[1]) == latestTime) {
                            String[] coordL = input2[2].trim().split("\\s+");
                            double tyLatL = Double.parseDouble(coordL[0]);
                            double tyLngL = Double.parseDouble(coordL[2]);
                            //if the end point of typhoon is west of 115E, then it belongs to group 3
                            if (tyLngL < 115)
                                groupId = "3";
                            //if the end point of typhoon is east of 115E, then it belongs to group 2
                            if (tyLngL > 115)
                                groupId = "2";
                        }
                    }
                }
                //if the start point of typhoon is east of 123E, then it may be group 2 or group 3
                if (tyLngE > 123)
                    groupId = "1";
            }
        }
        for (String e : cache) {
            context.write(null, new Text(groupId + "," + e));
        }

    }

}
