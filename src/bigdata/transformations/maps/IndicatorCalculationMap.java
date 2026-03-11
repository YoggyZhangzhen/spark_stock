package bigdata.transformations.maps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.function.PairFunction;

import bigdata.objects.StockPrice;
import bigdata.technicalindicators.Returns;
import bigdata.technicalindicators.Volitility;
import scala.Tuple2;

/**
 * Takes the grouped stock prices, sorts them chronologically, and calculates 
 * the Volatility (last 251 days) and Returns (last 5 days) for each stock.
 * Outputs a Tuple2 where Key = Ticker, Value = Tuple2<Volatility, Returns>.
 */
public class IndicatorCalculationMap implements PairFunction<Tuple2<String, Iterable<StockPrice>>, String, Tuple2<Double, Double>>{
    private static final long serialVersionUID = 1L;

    @Override
    public Tuple2<String, Tuple2<Double, Double>> call(Tuple2<String, Iterable<StockPrice>> groupedData) throws Exception{
        String ticker = groupedData._1;
        Iterable<StockPrice> pricesIterable = groupedData._2;

        // 1, convert Iterable to a standard java list
        List<StockPrice> prices = new ArrayList<>();
        for(StockPrice price: pricesIterable){
            prices.add(price);
        }

        // 2, Performation Optimization:sort the list chronologically
        // instead of converting to Instant, directly compare primitive intergers: Year -> Month -> Day
        Collections.sort(prices, new Comparator<StockPrice>(){
            @Override
            public int compare(StockPrice p1, StockPrice p2){
                int yearCmp = Integer.compare(p1.getYear(), p2.getYear());
                if(yearCmp != 0) return yearCmp;
                int monthCmp = Integer.compare(p1.getMonth(), p2.getMonth());
                if(monthCmp != 0) return monthCmp;

                return Integer.compare(p1.getDay(), p2.getDay());
            }
        });
        
        // 3, Extract the last 251 days for Volatility calculation
        // we should assume if there are not 251 price figures, and set the index to 0, same as return list
        List<Double> volList = new ArrayList<>();
        int volStartIndex = Math.max(0, prices.size() - 251);
        for(int i = volStartIndex; i< prices.size(); i++){
            volList.add(prices.get(i).getClosePrice());
        }

        // 4, Extract the last 5 days for Returns calculation
        List<Double> retList = new ArrayList<>();
        // get enough data and avoid index exception
        int retStartIndex = Math.max(0, prices.size() - 5 - 1);
        for(int i = retStartIndex; i< prices.size(); i++){
            retList.add(prices.get(i).getClosePrice());
        }
        // 5. Calculation indicators
        double volatility = Volitility.calculate(volList);
        double returns = Returns.calculate(5, retList);
        
        return new Tuple2<>(ticker, new Tuple2<>(volatility, returns));
    }
}
