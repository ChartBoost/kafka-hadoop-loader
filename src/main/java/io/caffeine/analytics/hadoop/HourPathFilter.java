package io.caffeine.analytics.hadoop;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.Date;

/**
 * HourPathFilter -
 * <p/>
 * User: kenny
 * Date: 1/19/14 1:17 PM
 */

public class HourPathFilter implements PathFilter, Configurable {
    private Configuration conf;

    @Override
    public boolean accept(Path path) {
        String name = path.getName();
        if (name.startsWith("_") || name.startsWith("."))
            return false;

        try {
            FileSystem fs = path.getFileSystem(new Configuration());
            FileStatus status = fs.getFileStatus(path);
            if (status.isDirectory())
                return true;

            Date now = new Date();
            Date dt = new Date(status.getModificationTime());
            int fileHour = dt.getHours();
            int endHour = now.getHours() - 1;
            int startHour = endHour - 1;

            if (conf != null) {
                endHour = conf.getInt("path.filter.hour.end", endHour);
                startHour = conf.getInt("path.filter.hour.start", endHour - 1);
            }

            boolean result = status.isFile() && status.getLen() > 0 && fileHour >= startHour && fileHour <= endHour;
            System.out.println("name: " + name + " length: " + status.getLen() + " result: " + result);
            return result;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void main(String args[]) throws IOException {
        PathFilter pf = new HourPathFilter();

        //Path path = new Path("s3n://AKIAJRTVXJ5SSA7MZTVQ:Ut1HKcZ3DCGQtQC2D4CAeHLaJdS0GQvszqlWIovo@scribe-json/click-json/2014-01-19/click*");
        Path path = new Path(args[0]);

        FileSystem fs = path.getFileSystem(new Configuration());
        FileStatus stats[] = fs.globStatus(path, pf);

        for (FileStatus status : stats) {
            System.out.println("name: " + status.getPath().getName() + " date: " + new Date(status.getModificationTime()));
        }
    }

    @Override
    public void setConf(Configuration c) {
        conf = c;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
