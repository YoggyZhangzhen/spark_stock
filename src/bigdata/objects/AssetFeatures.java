package bigdata.objects;

import java.io.Serializable;

public class AssetFeatures implements Serializable{

	private static final long serialVersionUID = -5189657923034324108L;
	
	double assetReturn;
	double assetVolitility;
	double peRatio;
	
	public AssetFeatures() {}
	
	public double getAssetReturn() {
		return assetReturn;
	}
	public void setAssetReturn(double assetReturn) {
		this.assetReturn = assetReturn;
	}
	public double getAssetVolitility() {
		return assetVolitility;
	}
	public void setAssetVolitility(double assetVolitility) {
		this.assetVolitility = assetVolitility;
	}
	public double getPeRatio() {
		return peRatio;
	}
	public void setPeRatio(double peRatio) {
		this.peRatio = peRatio;
	}

	
	
}
