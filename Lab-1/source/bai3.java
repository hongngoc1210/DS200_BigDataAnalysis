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

public class bai3 {

    private static BufferedReader openCacheFile(URI uri) throws IOException {
        return new BufferedReader(
                new InputStreamReader(new FileInputStream(uri.getPath())));
    }

    public static class GenderRatingMapper extends Mapper<Object, Text, Text, Text> {

        private final Map<String, String> movieTitleMap = new HashMap<>();
        private final Map<String, String> userGenderMap = new HashMap<>();

        private final Text movieTitleKey = new Text();
        private final Text genderRating  = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length < 2) return;

            try (BufferedReader br = openCacheFile(cacheFiles[0])) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    if (line.toLowerCase().startsWith("movieid")) continue;

                    String[] parts = line.split(",");
                    if (parts.length >= 2) {
                        movieTitleMap.put(parts[0].trim(), parts[1].trim());
                    }
                }
            }

            try (BufferedReader br = openCacheFile(cacheFiles[1])) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    if (line.toLowerCase().startsWith("userid")) continue;

                    String[] parts = line.split(",");
                    if (parts.length >= 2) {
                        userGenderMap.put(parts[0].trim(), parts[1].trim());
                    }
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;
            if (line.toLowerCase().startsWith("userid")) return; 

            String[] fields = line.split(",");
            if (fields.length < 3) return;

            try {
                String userID  = fields[0].trim();
                String movieID = fields[1].trim();
                double rating  = Double.parseDouble(fields[2].trim());

                String gender     = userGenderMap.get(userID);
                String movieTitle = movieTitleMap.get(movieID);

                if (gender == null || movieTitle == null) return;

                movieTitleKey.set(movieTitle);
                genderRating.set(gender + ":" + rating);
                context.write(movieTitleKey, genderRating);

            } catch (NumberFormatException e) {
                // pass
            }
        }
    }

    public static class GenderRatingReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            long   maleCount = 0,   femaleCount = 0;
            double maleSum   = 0.0, femaleSum   = 0.0;

            for (Text val : values) {
                String raw    = val.toString();
                int    sepIdx = raw.indexOf(":");
                if (sepIdx < 0) continue;

                String gender = raw.substring(0, sepIdx).trim();
                String rStr   = raw.substring(sepIdx + 1).trim();

                try {
                    double rating = Double.parseDouble(rStr);
                    if ("M".equalsIgnoreCase(gender)) {
                        maleSum += rating;
                        maleCount++;
                    } else if ("F".equalsIgnoreCase(gender)) {
                        femaleSum += rating;
                        femaleCount++;
                    }
                } catch (NumberFormatException e) {
                    // pass
                }
            }

            double maleAvg   = (maleCount  > 0) ? maleSum   / maleCount   : 0.0;
            double femaleAvg = (femaleCount > 0) ? femaleSum / femaleCount : 0.0;

            String outputVal = String.format("Male: %.2f, Female: %.2f", maleAvg, femaleAvg);
            context.write(key, new Text(outputVal));
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 5) {
            System.err.println("Usage: bai3 <ratings1> <ratings2> <movies> <users> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gender-Based Rating Analysis");

        job.setJarByClass(bai3.class);
        job.setMapperClass(GenderRatingMapper.class);
        job.setReducerClass(GenderRatingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, GenderRatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, GenderRatingMapper.class);

        // cache[0] = movies.txt, cache[1] = users.txt
        job.addCacheFile(new Path(args[2]).toUri());
        job.addCacheFile(new Path(args[3]).toUri());

        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

