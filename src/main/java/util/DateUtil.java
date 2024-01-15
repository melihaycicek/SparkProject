package util;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * Utility class for common date and time operations using Java 8's java.time API.
 */
public class DateUtil {

    private static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    private static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * Converts a date String to a LocalDate object.
     *
     * @param dateStr the date String in the format yyyy-MM-dd
     * @return the LocalDate object
     */
    public static LocalDate stringToDate(String dateStr) {
        return LocalDate.parse(dateStr, DateTimeFormatter.ofPattern(DEFAULT_DATE_FORMAT));
    }

    /**
     * Converts a datetime String to a LocalDateTime object.
     *
     * @param dateTimeStr the datetime String in the format yyyy-MM-dd HH:mm:ss
     * @return the LocalDateTime object
     */
    public static LocalDateTime stringToDateTime(String dateTimeStr) {
        return LocalDateTime.parse(dateTimeStr, DateTimeFormatter.ofPattern(DEFAULT_DATETIME_FORMAT));
    }

    /**
     * Converts a LocalDate object to a date String.
     *
     * @param date the LocalDate object
     * @return the date String in the format yyyy-MM-dd
     */
    public static String dateToString(LocalDate date) {
        return date.format(DateTimeFormatter.ofPattern(DEFAULT_DATE_FORMAT));
    }

    /**
     * Converts a LocalDateTime object to a datetime String.
     *
     * @param dateTime the LocalDateTime object
     * @return the datetime String in the format yyyy-MM-dd HH:mm:ss
     */
    public static String dateTimeToString(LocalDateTime dateTime) {
        return dateTime.format(DateTimeFormatter.ofPattern(DEFAULT_DATETIME_FORMAT));
    }

    /**
     * Calculates the number of days between two LocalDate objects.
     *
     * @param startDate the start date
     * @param endDate the end date
     * @return the number of days between the start and end dates
     */
    public static long daysBetween(LocalDate startDate, LocalDate endDate) {
        return ChronoUnit.DAYS.between(startDate, endDate);
    }

    /**
     * Gets the current date.
     *
     * @return the current LocalDate
     */
    public static LocalDate today() {
        return LocalDate.now();
    }

    /**
     * Gets the current datetime.
     *
     * @return the current LocalDateTime
     */
    public static LocalDateTime now() {
        return LocalDateTime.now();
    }

    // Additional utility methods can be added here...
}
