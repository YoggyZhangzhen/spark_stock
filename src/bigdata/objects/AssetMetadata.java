package bigdata.objects;

import java.io.Serializable;

import org.apache.spark.sql.Row;

/**
 * This is a pre-provided object that provides useful information about a financial asset,
 * such as its name, a description, as well as some numerical data.
 * 
 * The 'symbol' field is the same as the stock ticker
 * @author richardm
 *
 */
public class AssetMetadata implements Serializable {

	/*
	 *  INPUT EXAMPLE
	 *     {
        "address": "Via Lamarmora, 230 Brescia, BS 25124 Italy",
        "ceo": "Marco Emilio Angelo Patuano",
        "description": "A2A SpA together with its subsidiaries is engaged in the production, sale and distribution of electricity; sale and distribution of gas; production, distribution & sale of heat through district heating networks and integrated water cycle management.",
        "extraction_date": "07/01/2023",
        "figi": "BBG000DYQXH1",
        "growth": 21.87,
        "industry": "Utilities - Diversified",
        "isin": "IT0001233417",
        "name": "A2A SpA",
        "num_employees": 13511,
        "original_type": "common stock",
        "phone_no": "39 03 035531",
        "price_earning_ratio": 8.75,
        "sector": "Utilities",
        "style": "Mid-Blend",
        "symbol": "A",
        "website": "https://www.a2a.eu",
        "yield": 6.94
    }
	 */
	
	private static final long serialVersionUID = -8983149929196590048L;
	
	String symbol;
	String isin;
	String figi;
	
	String name;
	String description;
	
	String industry;
	String sector;
	
	double priceEarningRatio;
	double growth;
	
	/**
	 * Empty Constructor
	 */
	public AssetMetadata() {}
	
	/**
	 * Build an AssetMetadata object from a row
	 * @param assetRow
	 */
	public AssetMetadata(Row assetRow) {
		
		{int index = assetRow.fieldIndex("symbol"); if (!assetRow.isNullAt(index)) symbol = assetRow.getString(index);}
		{int index = assetRow.fieldIndex("isin"); if (!assetRow.isNullAt(index)) isin = assetRow.getString(index);}
		{int index = assetRow.fieldIndex("figi"); if (!assetRow.isNullAt(index)) figi = assetRow.getString(index);}
		{int index = assetRow.fieldIndex("name"); if (!assetRow.isNullAt(index)) name = assetRow.getString(index);}
		{int index = assetRow.fieldIndex("description"); if (!assetRow.isNullAt(index)) description = assetRow.getString(index);}
		{int index = assetRow.fieldIndex("industry"); if (!assetRow.isNullAt(index)) industry = assetRow.getString(index);}
		{int index = assetRow.fieldIndex("sector"); if (!assetRow.isNullAt(index)) sector = assetRow.getString(index);}
		
		try {int index = assetRow.fieldIndex("price_earning_ratio"); if (!assetRow.isNullAt(index)) priceEarningRatio = assetRow.getDouble(index);} catch (Exception e) {}
		try {int index = assetRow.fieldIndex("growth"); if (!assetRow.isNullAt(index)) growth = assetRow.getDouble(index);} catch (Exception e) {}
		
	}

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	public String getIsin() {
		return isin;
	}

	public void setIsin(String isin) {
		this.isin = isin;
	}

	public String getFigi() {
		return figi;
	}

	public void setFigi(String figi) {
		this.figi = figi;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getIndustry() {
		return industry;
	}

	public void setIndustry(String industry) {
		this.industry = industry;
	}

	public String getSector() {
		return sector;
	}

	public void setSector(String sector) {
		this.sector = sector;
	}

	public double getPriceEarningRatio() {
		return priceEarningRatio;
	}

	public void setPriceEarningRatio(double priceEarningRatio) {
		this.priceEarningRatio = priceEarningRatio;
	}

	public double getGrowth() {
		return growth;
	}

	public void setGrowth(double growth) {
		this.growth = growth;
	}
}
