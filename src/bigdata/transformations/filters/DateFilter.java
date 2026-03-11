package bigdata.transformations.filters;
import java.time.Instant;

import org.apache.spark.api.java.function.FilterFunction;
import bigdata.objects.StockPrice;
import bigdata.util.TimeUtil;

/**
 *  Filters out all price records that occur after a specific instant
 *  ensures we do not use future data to generate recommedations
 */
public class DateFilter implements FilterFunction<StockPrice>{
    private static final long serialVersionUID = 1L;
    private Instant endInstant;

    /**
     * Constructor to intialize the target end date, convert the string to an 
     * Instant object so it only runs once rather than pasing the string for every single row
     * @param datasetEndDate
     */
    public DateFilter(String datasetEndDate){
        this.endInstant = TimeUtil.fromDate(datasetEndDate);
    }

    @Override
    public boolean call(StockPrice price) throws Exception{
        // Parse the date of the current stock price row
        Instant curPriceDate = TimeUtil.fromDate(price.getYear(), price.getMonth(), price.getDay());

        // Return true to keep the record if the date is on or before the enddate
        return !curPriceDate.isAfter(this.endInstant);
    }
}
