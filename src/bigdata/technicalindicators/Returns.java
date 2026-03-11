package bigdata.technicalindicators;

import java.util.List;

/**
 * Returns the percentage change in price over the period considered
 * @author richardm
 *
 */
public class Returns {

	public static double calculate(int numDays, List<Double> closePrices) {
		if (closePrices.size() < numDays + 1) return 0d;

        Double previousPrice = closePrices.get(closePrices.size() - (numDays + 1));
        Double currentPrice = closePrices.get(closePrices.size() - 1);

        Double priceChange = currentPrice - previousPrice;
        return priceChange / previousPrice;

	}

}
