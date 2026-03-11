package bigdata.objects;

import java.io.Serializable;

public class AssetRanking implements Serializable{

	private static final long serialVersionUID = 4427588018088441273L;
	
	Asset[] assetRanking = null;
	
	public AssetRanking() {
		assetRanking = new Asset[5];
	}

	public AssetRanking(Asset[] assetRanking) {
		super();
		this.assetRanking = assetRanking;
	}

	public Asset[] getAssetRanking() {
		return assetRanking;
	}

	public void setAssetRanking(Asset[] assetRanking) {
		this.assetRanking = assetRanking;
	}
	
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for (int i=0; i<assetRanking.length; i++) {
			builder.append("Rank ");
			builder.append(i+1);
			builder.append(": ");
			
			Asset a = assetRanking[i];
			
			if (a==null) {
				builder.append("NULL\n");
				builder.append("\n");
				continue;
			}
			
			builder.append(a.getName());
			builder.append(" (");
			builder.append(a.getTicker());
			builder.append(")");
			builder.append("\n");
			builder.append("  - Industry: ");
			builder.append(a.getIndustry());
			builder.append("\n");
			builder.append("  - Sector: ");
			builder.append(a.getSector());
			builder.append("\n");
			builder.append("  - Returns: ");
			builder.append(a.getFeatures().getAssetReturn());
			builder.append("\n");
			builder.append("  - Volatility: ");
			builder.append(a.getFeatures().getAssetVolitility());
			builder.append("\n");
			builder.append("  - P/E Ratio: ");
			builder.append(a.getFeatures().getPeRatio());
			builder.append("\n");
			builder.append("\n");
		}
		
		return builder.toString();
	}
}
