package bigdata.objects;

import java.io.Serializable;

public class Asset implements Comparable<Asset>,Serializable{

	private static final long serialVersionUID = 3746176330609511622L;
	
	String ticker;
	AssetFeatures features;
	
	String name;
	String industry;
	String sector;
	
	public Asset() {}
	
	public Asset(String ticker, AssetFeatures features) {
		super();
		this.ticker = ticker;
		this.features = features;
	}

	public Asset(String ticker, AssetFeatures features, String name, String industry, String sector) {
		super();
		this.ticker = ticker;
		this.features = features;
		this.name = name;
		this.industry = industry;
		this.sector = sector;
	}

	public String getTicker() {
		return ticker;
	}

	public void setTicker(String ticker) {
		this.ticker = ticker;
	}

	public AssetFeatures getFeatures() {
		return features;
	}

	public void setFeatures(AssetFeatures features) {
		this.features = features;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
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

	@Override
	public int compareTo(Asset o) {
		return Double.compare(features.assetReturn, o.getFeatures().assetReturn);
	}
	
}
