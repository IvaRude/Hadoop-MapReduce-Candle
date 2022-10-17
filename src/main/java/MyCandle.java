import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class MyCandle {
    String slug;
    Date candle_start_time;
    float high;
    float low;
    float open_price;
    Date open_time;
    int open_id;
    float close_price;
    Date close_time;
    int close_id;
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");

    public MyCandle(){
        this.slug = "";
    }

    public MyCandle(String[] params, int candle_width) throws ParseException {
        this.slug = params[0];
        this.candle_start_time = find_candle_start_time(params[2], candle_width);
        this.high = Float.parseFloat(params[4]);
        this.low = this.high;
        this.open_price = this.high;
        this.open_time = dateFormat.parse(params[2]);
        this.open_id = Integer.parseInt(params[3]);
        this.close_price = this.open_price;
        this.close_time = this.open_time;
        this.close_id = this.open_id;
    }

    public MyCandle(HashMap<String, String> params, int candle_width) throws ParseException {
        this.slug = params.get("slug");
        this.candle_start_time = find_candle_start_time(params.get("date"), candle_width);
        this.high = Float.parseFloat(params.get("price"));
        this.low = this.high;
        this.open_price = this.high;
        this.open_time = dateFormat.parse(params.get("date"));
        this.open_id = Integer.parseInt(params.get("id"));
        this.close_price = this.open_price;
        this.close_time = this.open_time;
        this.close_id = this.open_id;
    }

    public Date find_candle_start_time(String date_str, int candle_width) throws ParseException {
        String start_str = date_str.substring(0, 8).concat("000000000");
        Date start = dateFormat.parse(start_str);
        Date date = dateFormat.parse(date_str);
        long milliseconds = date.getTime() - start.getTime();
        long num_of_interval = milliseconds / candle_width;
        return new Date(start.getTime() + num_of_interval * candle_width);
    }

    public MyCandle add_data(MyCandle candle) {
        this.high = max(this.high, candle.high);
        this.low = min(this.low, candle.low);
        if (candle.open_time.getTime() < this.open_time.getTime() ||
                (candle.open_time.getTime() == this.open_time.getTime() && candle.open_id < this.open_id)) {
            this.open_id = candle.open_id;
            this.open_time = candle.open_time;
            this.open_price = candle.open_price;
        }
        if (candle.close_time.getTime() > this.close_time.getTime() ||
                (candle.close_time.getTime() == this.close_time.getTime() && candle.close_id > this.close_id)) {
            this.close_id = candle.close_id;
            this.close_time = candle.close_time;
            this.close_price = candle.close_price;
        }
        return this;
    }

    public String convert_to_string_for_mapper() {
        String ans = this.slug;
        ans = ans.concat("," + String.valueOf(this.high));
        ans = ans.concat("," + String.valueOf(this.low));
        ans = ans.concat("," + String.valueOf(this.open_price));
        ans = ans.concat("," + this.dateFormat.format(this.open_time));
        ans = ans.concat("," + String.valueOf(this.open_id));
        ans = ans.concat("," + String.valueOf(this.close_price));
        ans = ans.concat("," + this.dateFormat.format(this.close_time));
        ans = ans.concat("," + String.valueOf(this.close_id));
        return ans;
    }

    public static MyCandle convert_from_string_to_candle(String string) throws ParseException {
        MyCandle candle = new MyCandle();
        String[] params = string.toString().split(",");
        candle.slug = params[0];
        candle.high = Float.parseFloat(params[1]);
        candle.low = Float.parseFloat(params[2]);
        candle.open_price = Float.parseFloat(params[3]);
        candle.open_time = candle.dateFormat.parse(params[4]);
        candle.open_id = Integer.parseInt(params[5]);
        candle.close_price = Float.parseFloat(params[6]);
        candle.close_time = candle.dateFormat.parse(params[7]);
        candle.close_id = Integer.parseInt(params[8]);
        return candle;
    }
}
