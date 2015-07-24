
import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class Main {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length!=2){
            System.out.println("Usage: <input> <output>");
            System.exit(1);
        }
        //basic settings of MapReduce Job
        Job job1 = Job.getInstance(new Configuration());
        Job job2 = Job.getInstance(new Configuration());
        Job job3 = Job.getInstance(new Configuration());

        job1.setMapperClass(map1.class);
        job1.setReducerClass(reduce1.class);

        job2.setMapperClass(map2.class);
        job2.setReducerClass(reduce2.class);

        job3.setMapperClass(map3.class);
        job3.setReducerClass(reduce3.class);

        job1.setJarByClass(Main.class);
        job2.setJarByClass(Main.class);
        job3.setJarByClass(Main.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        Path temp1 = new Path("BigDataProject" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        Path temp2 = new Path("BigDataProject" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,temp1);

        FileInputFormat.setInputPaths(job2, temp1);
        FileOutputFormat.setOutputPath(job2,temp2);

        FileInputFormat.setInputPaths(job3, temp2);
        FileOutputFormat.setOutputPath(job3,new Path(args[1]));

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job1.setNumReduceTasks(1);
        job2.setNumReduceTasks(1);
        job3.setNumReduceTasks(1);

        if (job1.waitForCompletion(true)){
        }
        if (job2.waitForCompletion(true)){
        }
        System.exit(job3.waitForCompletion(true)? 0:1);

    }
}
