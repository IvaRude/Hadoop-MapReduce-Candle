import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class candle extends Configured implements Tool {
    static int candle_width = 300000;
    static String candle_securities = ".*";
    static String candle_date_from = "19000101";
    static String candle_date_to = "20200101";
    static String candle_time_from = "1000";
    static String candle_time_to = "1800";
    static int num_of_redusers = 1;

    public void parse_args(String[] args) {
        int ind = 2;
        while (ind < args.length) {
            if (args[ind].equals("-D")) {
                ind += 1;
                String[] kwargs = args[ind].split("=");
                String key = kwargs[0];
                switch (key) {
                    case "candle.width":
                        candle_width = Integer.parseInt(kwargs[1]);
                        break;
                    case "candle.securities":
                        candle_securities = kwargs[1];
                        break;
                    case "candle.date.from":
                        candle_date_from = kwargs[1];
                        break;
                    case "candle.date.to":
                        candle_date_to = kwargs[1];
                        break;
                    case "candle.time.from":
                        candle_time_from = kwargs[1];
                        break;
                    case "candle.time.to":
                        candle_time_to = kwargs[1];
                        break;
                    case "candle.num.reducers":
                        num_of_redusers = Integer.parseInt(kwargs[1]);
                        break;
                }
                ind += 1;
            }
        }
    }

    public Configuration feel_configuration(Configuration conf) {
        if (conf.get("candle.width") == null) conf.set("candle.width", Integer.toString(candle_width));
        if (conf.get("candle.securities") == null) conf.set("candle.securities", candle_securities);
        if (conf.get("candle.date.from") == null) conf.set("candle.date.from", candle_date_from);
        if (conf.get("candle.date.to") == null) conf.set("candle.date.to", candle_date_to);
        if (conf.get("candle.time.from") == null) conf.set("candle.time.from", candle_time_from);
        if (conf.get("candle.time.to") == null) conf.set("candle.time.to", candle_time_to);
        return conf;
    }


    public static class CandleMapper
            extends Mapper<Object, Text, Text, Text> {
        public HashMap<String, MyCandle> candles;
        public Configuration conf;
        static int candle_width;
        static String candle_securities;
        static String candle_date_from;
        static String candle_date_to;
        static String candle_time_from;
        static String candle_time_to;

        public void get_params_from_conf(Configuration conf) {
            if (conf.get("candle.width") != null) candle_width = Integer.parseInt(conf.get("candle.width"));
            if (conf.get("candle.securities") != null) candle_securities = conf.get("candle.securities");
            if (conf.get("candle.date.from") != null) candle_date_from = conf.get("candle.date.from");
            if (conf.get("candle.date.to") != null) candle_date_to = conf.get("candle.date.to");
            if (conf.get("candle.time.from") != null) candle_time_from = conf.get("candle.time.from");
            if (conf.get("candle.time.to") != null) candle_time_to = conf.get("candle.time.to");
        }

        public HashMap<String, String> parse_input_params(String[] params) {
            HashMap<String, String> candle_params = new HashMap<>();
            for (String param : params) {
                if (param.length() == 17) {
                    candle_params.put("date", param);
                } else if (param.contains(".")) {
                    candle_params.put("price", param);
                } else if (param.length() == 4) {
                    try {
                        int num = Integer.parseInt(param);
                    }
                    catch (NumberFormatException e) {
                        candle_params.put("slug", param);
                    }
                } else if (param.length() > 6) {
                    candle_params.put("id", param);
                }
            }
            return candle_params;
        }

        public boolean is_satisfying_candle(MyCandle candle) {
            if (!Pattern.matches(candle_securities, candle.slug)) {
                return false;
            }
            String date = candle.dateFormat.format(candle.candle_start_time).substring(0, 8);
            String time = candle.dateFormat.format(candle.candle_start_time).substring(8, 12);
            if (date.compareTo(candle_date_from) < 0 || date.compareTo(candle_date_to) >= 0) {
                return false;
            }
            if (time.compareTo(candle_time_from) < 0 || time.compareTo(candle_time_to) >= 0) {
                return false;
            }
            return true;
        }

        public void setup(Context context) {
            this.candles = new HashMap<>();
            this.conf = context.getConfiguration();
            get_params_from_conf(this.conf);
        }

        public void map(Object key, Text value, Context context) {
            if (value.toString().contains("SYMBOL")) return;
            String[] params = value.toString().split(",");
            MyCandle candle = null;
            try {
                candle = new MyCandle(parse_input_params(params), candle_width);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            if (!is_satisfying_candle(candle)) return;
            String candle_key = params[0].concat(candle.dateFormat.format(candle.candle_start_time));
            if (this.candles.containsKey(candle_key)) {
                this.candles.put(candle_key, this.candles.get(candle_key).add_data(candle));
            } else {
                this.candles.put(candle_key, candle);
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, MyCandle> entry : this.candles.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue().convert_to_string_for_mapper();
                context.write(new Text(key), new Text(value));
            }
        }
    }

    public static class CandleReducer
            extends Reducer<Text, Text, NullWritable, Text> {
        private MultipleOutputs mos;

        public void setup(Context context) {
            mos = new MultipleOutputs(context);
        }

        public Text make_output_value(String key, MyCandle candle) {
            String date = key.substring(key.length() - 17);
            String ans = candle.slug;
            ans = ans.concat("," + date);
            ans = ans.concat("," + String.valueOf(BigDecimal.valueOf(candle.open_price).setScale(1, BigDecimal.ROUND_HALF_UP).floatValue()));
            ans = ans.concat("," + String.valueOf(BigDecimal.valueOf(candle.high).setScale(1, BigDecimal.ROUND_HALF_UP).floatValue()));
            ans = ans.concat("," + String.valueOf(BigDecimal.valueOf(candle.low).setScale(1, BigDecimal.ROUND_HALF_UP).floatValue()));
            ans = ans.concat("," + String.valueOf(BigDecimal.valueOf(candle.close_price).setScale(1, BigDecimal.ROUND_HALF_UP).floatValue()));
            return new Text(ans);
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            MyCandle candle = new MyCandle();
            candle.slug = "";
            for (Text val : values) {
                try {
                    MyCandle new_candle = MyCandle.convert_from_string_to_candle(val.toString());
                    if (candle.slug.equals("")) {
                        candle = new_candle;
                    } else {
                        candle.add_data(new_candle);
                    }
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }
            Text output_value = make_output_value(key.toString(), candle);
            mos.write(NullWritable.get(), output_value, candle.slug);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(67108864));
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        parse_args(otherArgs);
        conf = feel_configuration(conf);
        Job job = new Job(conf, "candle");
        if (conf.get("candle.num.reducers") == null)
            job.setNumReduceTasks(num_of_redusers);
        else
            job.setNumReduceTasks(Integer.parseInt(conf.get("candle.num.reducers")));

        job.setJarByClass(candle.class);
        job.setMapperClass(CandleMapper.class);
        job.setReducerClass(CandleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new candle(), args);
        System.exit(res);
    }
}
