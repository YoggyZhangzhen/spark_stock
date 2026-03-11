package bigdata.transformations.maps;

import org.apache.spark.api.java.function.PairFunction;

import bigdata.objects.StockPrice;
import scala.Tuple2;

/**
 * Maps each StockPrice to a k-v pair, key is stock ticker and v is the stockprice, prepare for groupByKey
 */
public class PricePairingMap implements PairFunction<StockPrice, String, StockPrice>{
    private static final long serialVersionUID = 1L;

    @Override
    public Tuple2<String, StockPrice> call(StockPrice price) throws Exception{
        // Extract the stock ticker as key, and package it with price object
        return new Tuple2<>(price.getStockTicker(), price);
    }
}
