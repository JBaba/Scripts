import java.sql.Timestamp;
import java.text.DateFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;


/**
 * Function avoids weekend days and bank holidays.
 * @author nviradia
 *
 */
public class DateAfterBankingHolidays {
	
	public String[] bankHolidays2016 = {"1/1/2016","1/18/2016","2/15/2016","5/30/2016","7/4/2016","9/5/2016","10/10/2016",
			"11/11/2016","11/24/2016","12/26/2016"};

	public void findNextDate(String pramDate,String jobid) throws Exception{
		
		SimpleDateFormat date = new SimpleDateFormat("MM/dd/yyyy");
		Timestamp[] timeStampBankHolidays2016 = new Timestamp[bankHolidays2016.length];
		int index = 0;
		for(String day:bankHolidays2016){
			long time = date.parse(day).getTime();
			timeStampBankHolidays2016[index] = new Timestamp(time);
			Calendar cal = new GregorianCalendar();
			cal.setTime(timeStampBankHolidays2016[index]);
			
			// Then get the day of week from the Date based on specific locale.
			String dayOfWeek = new SimpleDateFormat("EEEE", Locale.ENGLISH).format(new Date(time));
			//System.out.println(day+":"+dayOfWeek); 
			
			Locale usersLocale = Locale.getDefault();
			DateFormatSymbols dfs = new DateFormatSymbols(usersLocale);
			String weekdays[] = dfs.getWeekdays();
			int dayNum = cal.get(Calendar.DAY_OF_WEEK);
			dayOfWeek = weekdays[dayNum];
			//System.out.println(day+":"+dayOfWeek); 
			
			index++;
		}
		
		System.out.println(date.parse(pramDate).getTime());
		Timestamp paramTs = new Timestamp(date.parse(pramDate).getTime());
		Timestamp foundDay = getBankBusinessDayAfterNDays(paramTs, 2, timeStampBankHolidays2016,jobid);
		System.out.println(pramDate+":"+foundDay);
	}
	
	/**
	 * Method returns a bank business day n days from today. It skips bank holidays based
	 * on the information in BANKHOLIDAYS reference table. Ensure that holidays are
	 * tabulated in the BANKHOLIDAYS reference table for this to work as expected.
	 * 
	 * @param int - days from today
	 * @return Timestamp - Timestamp of the day n days from today
	 */
	public static Timestamp getBankBusinessDayAfterNDays(Timestamp input, int n,Timestamp[] timeStampBankHolidays,String jobid) {
        Calendar cal = new GregorianCalendar();
        Timestamp ts = new Timestamp(input.getTime());
        Timestamp oldDate = new Timestamp(ts.getTime());
        
        cal.setTime(ts);
        
        cal.set(Calendar.HOUR, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        ts.setTime(cal.getTime().getTime());
        
        while (n > 0) {
              cal.setTime(ts);
              cal.add(Calendar.DAY_OF_MONTH, 1);

              while (cal.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY
                          || cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
                    cal.add(Calendar.DAY_OF_MONTH, 1);
              }
              
              for(Timestamp compareDate:timeStampBankHolidays){
            	  long nextTime = cal.getTime().getTime();
            	  if(compareDate.compareTo(new Timestamp(nextTime)) == 0){
            		  cal.add(Calendar.DAY_OF_MONTH, 1);
            	  }
              }
              
              while (cal.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY
                      || cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
            	  cal.add(Calendar.DAY_OF_MONTH, 1);
              }

              ts.setTime(cal.getTime().getTime());
              n--;
        }
        
        // Find next month date except weekends and holidays
        if(jobid.equalsIgnoreCase("IN-EFTS-MLY")){
        	System.out.println("It's monthly :"+ts);
        	if(oldDate.getMonth() == ts.getMonth()){
        		Calendar nextMonthDate = new GregorianCalendar();
        		nextMonthDate.setTime(ts);
        		nextMonthDate.add(Calendar.MONTH, 1);
        		nextMonthDate.set(Calendar.DATE, nextMonthDate.getActualMinimum(Calendar.DAY_OF_MONTH));
        		
        		 while (nextMonthDate.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY
	                          || nextMonthDate.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
        			 nextMonthDate.add(Calendar.DAY_OF_MONTH, 1);
	              }
	              
	              for(Timestamp compareDate:timeStampBankHolidays){
	            	  long nextTime = nextMonthDate.getTime().getTime();
	            	  if(compareDate.compareTo(new Timestamp(nextTime)) == 0){
	            		  nextMonthDate.add(Calendar.DAY_OF_MONTH, 1);
	            	  }
	              }
	              
	              while (nextMonthDate.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY
	                      || nextMonthDate.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
	            	  nextMonthDate.add(Calendar.DAY_OF_MONTH, 1);
	              }
	              
	              ts.setTime(nextMonthDate.getTime().getTime());
        	}
        }
        
        return ts;
	}
	
	public static void main(String[] args) throws Exception {
		DateAfterBankingHolidays dab = new DateAfterBankingHolidays();
		dab.findNextDate("12/30/2015","DLY");
		System.out.println(":------------------------------:");
		dab.findNextDate("1/14/2016","DLY");
		System.out.println(":------------------------------:");
		dab.findNextDate("1/15/2016","DLY");
		System.out.println(":------------------------------:");
		dab.findNextDate("11/23/2016","DLY");
		System.out.println(":------------------------------:");
		dab.findNextDate("11/22/2016","DLY");
		System.out.println(":------------------------------:");
		dab.findNextDate("12/28/2016","DLY");
		System.out.println(":------------------------------:");
		dab.findNextDate("12/25/2015","IN-EFTS-MLY");
		System.out.println(":------------------------------:");
		dab.findNextDate("5/27/2016","IN-EFTS-MLY");
		System.out.println(":------------------------------:");
		dab.findNextDate("5/30/2016","IN-EFTS-MLY");
	}

}
