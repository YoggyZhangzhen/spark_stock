package bigdata.transformations.pairing;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;

import bigdata.objects.AssetMetadata;
import scala.Tuple2;

/**
 * This converts from an Apache Spark row to an AssetMetadata java object that is
 * more convenient to work with,
 * @author richardm
 *
 */
public class AssetMetadataPairing implements PairFunction<Row,String,AssetMetadata>{

	private static final long serialVersionUID = 8370920503065852040L;

	@Override
	public Tuple2<String, AssetMetadata> call(Row row) throws Exception {
		AssetMetadata assetMeta = new AssetMetadata(row);
		return new Tuple2<String, AssetMetadata>(assetMeta.getSymbol(), assetMeta);
	}


}
