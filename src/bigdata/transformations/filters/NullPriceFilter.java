package bigdata.transformations.filters;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

/**
 * Some of the prices are null for some days, we need to filter them out to avoid getting
 * exceptions thrown.
 */
public class NullPriceFilter implements FilterFunction<Row> {

	private static final long serialVersionUID = -2353664115853315391L;

	@Override
	public boolean call(Row row) throws Exception {
		// if we find a null entry, filter out the row
		if (row.getString(0)==null) return false;
		if (row.getString(1)==null) return false;
		if (row.getString(2)==null) return false;
		if (row.getString(3)==null) return false;
		if (row.getString(4)==null) return false; 
		if (row.getString(5)==null) return false; 
		if (row.getString(6)==null) return false; 
		if (row.getString(7)==null) return false; 
		return true;
	}

}
