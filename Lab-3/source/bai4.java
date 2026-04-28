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

public class bai4 {

    private static BufferedReader openCacheFile(URI uri) throws IOException {
        return new BufferedReader(
                new InputStreamReader(new FileInputStream(uri.getPath())));
    }

    private static String getAgeGroup(int age) {
        if (age < 18)      return "0-18";
        else if (age < 35) return "18-35";
        else if (age < 50) return "35-50";
        else               return "50+";
    }


    public static class AgeRatingMapper extends Mapper<Object, Text, Text, Text> {

        private final Map<String, String> movieTitleMap = new HashMap<>();
        private final Map<String, String> userAgeMap    = new HashMap<>();

        private final Text movieTitleKey = new Text();
        private final Text ageRating     = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length < 2) return;

            // Schema: MovieID,Title,Genres
            try (BufferedReader br = openCacheFile(cacheFiles[0])) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    if (line.toLowerCase().startsWith("movieid")) continue; // bỏ header

                    String[] parts = line.split(",");
                    if (parts.length >= 2) {
                        movieTitleMap.put(parts[0].trim(), parts[1].trim());
                    }
                }
            }

            // Schema: UserID,Gender,Age,Occupation,Zip
            try (BufferedReader br = openCacheFile(cacheFiles[1])) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    if (line.toLowerCase().startsWith("userid")) continue; // bỏ header

                    String[] parts = line.split(",");
                    if (parts.length >= 3) {
                        // parts[0]=UserID, parts[2]=Age
                        userAgeMap.put(parts[0].trim(), parts[2].trim());
                    }
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;
            if (line.toLowerCase().startsWith("userid")) return; // bỏ header

            String[] fields = line.split(",");
            if (fields.length < 3) return;

            try {
                String userID  = fields[0].trim();
                String movieID = fields[1].trim();
                double rating  = Double.parseDouble(fields[2].trim());

                String ageStr     = userAgeMap.get(userID);
                String movieTitle = movieTitleMap.get(movieID);

                if (ageStr == null || movieTitle == null) return;

                int    age      = Integer.parseInt(ageStr);
                String ageGroup = getAgeGroup(age);

                movieTitleKey.set(movieTitle);
                ageRating.set(ageGroup + ":" + rating);
                context.write(movieTitleKey, ageRating);

            } catch (NumberFormatException e) {
            }
        }
    }

    public static class AgeRatingReducer extends Reducer<Text, Text, Text, Text> {

        private static final String[] AGE_GROUPS = {"0-18", "18-35", "35-50", "50+"};

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<String, Double> sumMap   = new HashMap<>();
            Map<String, Long>   countMap = new HashMap<>();

            for (String g : AGE_GROUPS) {
                sumMap.put(g, 0.0);
                countMap.put(g, 0L);
            }

            for (Text val : values) {
                String raw    = val.toString();
                int    sepIdx = raw.lastIndexOf(":");
                if (sepIdx < 0) continue;

                String group = raw.substring(0, sepIdx).trim();
                String rStr  = raw.substring(sepIdx + 1).trim();

                if (!sumMap.containsKey(group)) continue;

                try {
                    double rating = Double.parseDouble(rStr);
                    sumMap.put(group,   sumMap.get(group)   + rating);
                    countMap.put(group, countMap.get(group) + 1);
                } catch (NumberFormatException e) {
                    // pass
                }
            }

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < AGE_GROUPS.length; i++) {
                String g     = AGE_GROUPS[i];
                long   count = countMap.get(g);
                if (i > 0) sb.append("  ");
                if (count > 0) {
                    double avg = sumMap.get(g) / count;
                    sb.append(String.format("%s: %.2f", g, avg));
                } else {
                    sb.append(g).append(": NA");
                }
            }

            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 5) {
            System.err.println("Usage: bai4 <ratings1> <ratings2> <movies> <users> <o>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Age Group Rating Analysis");

        job.setJarByClass(bai4.class);
        job.setMapperClass(AgeRatingMapper.class);
        job.setReducerClass(AgeRatingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, AgeRatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, AgeRatingMapper.class);

        // cache[0] = movies.txt, cache[1] = users.txt
        job.addCacheFile(new Path(args[2]).toUri());
        job.addCacheFile(new Path(args[3]).toUri());

        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}