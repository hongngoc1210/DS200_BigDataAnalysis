package source;
import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai1 {

    public static class RatingMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private final Text movieIDKey = new Text();
        private final DoubleWritable ratingValue = new DoubleWritable();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;
            // Bỏ qua header
            if (line.toLowerCase().startsWith("userid")) return;

            String[] parts = line.split(",");
            if (parts.length < 3) return;

            try {
                String movieID = parts[1].trim();
                double rating = Double.parseDouble(parts[2].trim());
                movieIDKey.set(movieID);
                ratingValue.set(rating);
                context.write(movieIDKey, ratingValue);
            } catch (NumberFormatException e) {
                // pass
            }
        }
    }

    public static class RatingReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        private static String maxMovie = "";
        private static double maxRating = -1.0;
        private final Map<String, String> movieTitleMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) return;

            String fileName = new Path(cacheFiles[0].toString()).getName();
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream(fileName)))) {
                String line;
                boolean firstLine = true;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    // Bỏ qua header
                    if (firstLine && line.toLowerCase().startsWith("movieid")) {
                        firstLine = false;
                        continue;
                    }
                    firstLine = false;
                    String[] parts = line.split(",");
                    if (parts.length >= 2) {
                        movieTitleMap.put(parts[0].trim(), parts[1].trim());
                    }
                }
            }
        }

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            long count = 0;
            double sum = 0.0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            double avg = (count > 0) ? sum / count : 0.0;
            String movieID    = key.toString();
            String movieTitle = movieTitleMap.getOrDefault(movieID, movieID);

            if (count >= 5 && avg > maxRating) {
                maxRating = avg;
                maxMovie  = movieTitle;
            }

            String output = String.format("Average rating: %.1f (Total ratings: %d)", avg, count);
            context.write(new Text(movieTitle), new Text(output));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!maxMovie.isEmpty()) {
                String msg = String.format(
                    "%s is the highest rated movie with an average rating of %.1f among movies with at least 5 ratings.",
                    maxMovie, maxRating);
                context.write(new Text(">>> TOP RATED >>>"), new Text(msg));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: bai1 <ratings1> <ratings2> <movies> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Movie Rating");
        job.setJarByClass(bai1.class);

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        // Fix: MapOutputValueClass phải là DoubleWritable
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

        job.addCacheFile(new Path(args[2]).toUri());

        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}