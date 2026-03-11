package bigdata.transformations.maps;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import bigdata.objects.StockPrice;

/**
 * This is a simple Map transformer that converts from Spark SQL Row objects into
 * a custom StockPrice object to make it nicer to work with
 * @author richardm
 *
 */
public class PriceReaderMap implements MapFunction<Row,StockPrice>{

	private static final long serialVersionUID = -1855834065277055593L;

	@Override
	public StockPrice call(Row row) throws Exception {
		StockPrice priceData = new StockPrice(row);

		//System.err.println(priceData.toString());
		
		return priceData;
	}

	
}


