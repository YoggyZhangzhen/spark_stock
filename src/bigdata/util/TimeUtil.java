package bigdata.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class TimeUtil {

	private static DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_DATE_TIME;
	private static ZoneId zoneId = ZoneId.of("Europe/London");
	
	public static Instant fromDate(short year, short month, short day) {
		
		StringBuilder date = new StringBuilder();
		date.append(String.valueOf(year));
		date.append("-");
		
		String monthS = String.valueOf(month);
		if (monthS.length()==1) date.append("0");
		date.append(monthS);
		date.append("-");
		
		String dayS = String.valueOf(day);
		if (dayS.length()==1) date.append("0");
		date.append(dayS);
		
		date.append("T00:00:00Z");
		
		LocalDateTime localDateTime = LocalDateTime.parse(date.toString(), dateFormatter);
		ZonedDateTime zonedDateTime = localDateTime.atZone(zoneId); 
		return zonedDateTime.toInstant();
	}
	
	public static Instant fromDate(String dateString) {
		
		String[] dateParts = dateString.split("-");
		StringBuilder date = new StringBuilder();
		date.append(dateParts[0]);
		date.append("-");
		
		String monthS = dateParts[1];
		if (monthS.length()==1) date.append("0");
		date.append(monthS);
		date.append("-");
		
		String dayS = dateParts[2];
		if (dayS.length()==1) date.append("0");
		date.append(dayS);
		
		date.append("T00:00:00Z");
		
		LocalDateTime localDateTime = LocalDateTime.parse(date.toString(), dateFormatter);
		ZonedDateTime zonedDateTime = localDateTime.atZone(zoneId); 
		return zonedDateTime.toInstant();
		
	}
	
}
