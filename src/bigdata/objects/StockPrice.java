package bigdata.objects;

import java.io.Serializable;

import org.apache.spark.sql.Row;

/**
 * This object represents the pricing data for a single stock on a particular day.
 * @author richardm
 *
 */
public class StockPrice implements Serializable{

	private static final long serialVersionUID = -5196324574222281643L;
	
	// Date,Open,High,Low,Close,Adj Close,Volume,Stock
	
	// timestamp
	short year; // Year
	short month; // Month (range 1-12)
	short day; // Day
	
	// price data
	double openPrice; // Price at Market Open
	double highPrice; // Highest Price during the Day
	double lowPrice; // Lowest Price during the Day
	double closePrice; // Closing Price
	
	double adjustedClosePrice; // Adjusted Closing Price
	
	double volume; // Number of Units Traded during the Day
	
	String stockTicker; // Company Stock Ticker
	
	// Empty constructor required by Spark Encoders for reflection！
    public StockPrice() {
    }
	/**
	 * Builds a StockPrice instance based on a Spark SQL row, which is what is read from the input file.
	 * The input file is in CSV format, where each row contains: Date,Open,High,Low,Close,Adj Close,Volume,Stock 
	 */
	public StockPrice(Row row) {
		
		
		
		try {
			// Perform manual date parsing
			String[] dateString = row.getString(0).split("-"); // e.g. 1999-11-18
			year = Short.parseShort(dateString[0]);
			month = Short.parseShort(dateString[1]);
			day = Short.parseShort(dateString[2]);
			
			// Parse Prices
			openPrice = Double.parseDouble(row.getString(1));
			highPrice = Double.parseDouble(row.getString(2));
			lowPrice = Double.parseDouble(row.getString(3));
			closePrice = Double.parseDouble(row.getString(4));
			adjustedClosePrice = Double.parseDouble(row.getString(5));
			
			// Parse Volume Traded
			volume = Double.parseDouble(row.getString(6));
			
			// Parse Stock Ticker
			stockTicker = row.getString(7);
			
			
		} catch (Exception e) {
			System.err.println(row.toString());
			e.printStackTrace();
		}
	}

	public short getYear() {
		return year;
	}

	public void setYear(short year) {
		this.year = year;
	}

	public short getMonth() {
		return month;
	}

	public void setMonth(short month) {
		this.month = month;
	}

	public short getDay() {
		return day;
	}

	public void setDay(short day) {
		this.day = day;
	}

	public double getOpenPrice() {
		return openPrice;
	}

	public void setOpenPrice(double openPrice) {
		this.openPrice = openPrice;
	}

	public double getHighPrice() {
		return highPrice;
	}

	public void setHighPrice(double highPrice) {
		this.highPrice = highPrice;
	}

	public double getLowPrice() {
		return lowPrice;
	}

	public void setLowPrice(double lowPrice) {
		this.lowPrice = lowPrice;
	}

	public double getClosePrice() {
		return closePrice;
	}

	public void setClosePrice(double closePrice) {
		this.closePrice = closePrice;
	}

	public double getAdjustedClosePrice() {
		return adjustedClosePrice;
	}

	public void setAdjustedClosePrice(double adjustedClosePrice) {
		this.adjustedClosePrice = adjustedClosePrice;
	}

	public double getVolume() {
		return volume;
	}

	public void setVolume(double volume) {
		this.volume = volume;
	}

	public String getStockTicker() {
		return stockTicker;
	}

	public void setStockTicker(String stockTicker) {
		this.stockTicker = stockTicker;
	}
	
	public String toString() {
		StringBuilder textBuffer = new StringBuilder();
		textBuffer.append(day);
		textBuffer.append("/");
		textBuffer.append(month);
		textBuffer.append("/");
		textBuffer.append(year);
		textBuffer.append(" ");
		textBuffer.append(openPrice);
		textBuffer.append(" ");
		textBuffer.append(highPrice);
		textBuffer.append(" ");
		textBuffer.append(lowPrice);
		textBuffer.append(" ");
		textBuffer.append(closePrice);
		textBuffer.append(" ");
		textBuffer.append(adjustedClosePrice);
		textBuffer.append(" ");
		textBuffer.append(volume);
		textBuffer.append(" ");
		textBuffer.append(stockTicker);
		
		return textBuffer.toString();
	}

	
}
