package foa;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.util.Locale;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

public class Util {

	public static String formatDecimal(Double val, String pattern) {
		if (val == null)
			return "";
		return new DecimalFormat(pattern, new DecimalFormatSymbols(new Locale("pt", "BR"))).format(val);
	}
	
	public static String formatJsonDate(String dateStr, String pattern) {
		try {
			return DateFormatUtils.format(DateUtils.parseDate(dateStr, "yyyy-MM-dd"), pattern);
		} catch (ParseException e) {
			return null;
		}
	}
	
}
