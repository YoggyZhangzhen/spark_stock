package bigdata.technicalindicators;

import java.util.ArrayList;
import java.util.List;

import bigdata.util.MathUtils;

public class Volitility {

	public static double calculate(List<Double> closePrices) {
		if (closePrices.size() < 2) return 0d;

        List<Double> currentClose = closePrices.subList(1, closePrices.size());
        List<Double> previousClose = closePrices.subList(0, closePrices.size() - 1);

        List<Double> volitilities = new ArrayList<Double>(currentClose.size());
        for (int i=0; i<currentClose.size(); i++) {
        	double currentClosePrice = currentClose.get(i);
        	double previousClosePrice = previousClose.get(i);
        	
        	double vol = (currentClosePrice - previousClosePrice) / previousClosePrice;
        	if (vol>0) vol = Math.log(vol);
        	else if (vol<0) vol = -Math.log(-vol);
        	
        	volitilities.add(vol);
        }
  

        return MathUtils.std(volitilities);


	}

}

