package io.kestra.plugin.influxdb.utils;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

public class TimeUtils {
    private static final Pattern NUMERIC_PATTERN = Pattern.compile("^\\d+$");
    private static final Pattern NEEDS_TIMEZONE = Pattern.compile("^(?!.*[Z+-]).*");

    private static final List<DateTimeFormatter> FORMATTERS = List.of(
        DateTimeFormatter.ISO_INSTANT,
        DateTimeFormatter.ISO_OFFSET_DATE_TIME,
        DateTimeFormatter.ISO_ZONED_DATE_TIME,
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm[:ss][.SSS][XXX][VV]"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm[:ss][.SSS][XXX][VV]"),
        DateTimeFormatter.ISO_LOCAL_DATE_TIME,
        DateTimeFormatter.ISO_LOCAL_DATE
    );

    private TimeUtils() {
        // prevent instantiation
    }

    public static Instant toInstant(Object input) {
        if (input == null) return null;

        return switch (input) {
            case Instant instant -> instant;
            case ZonedDateTime zdt -> zdt.toInstant();
            case OffsetDateTime odt -> odt.toInstant();
            case LocalDateTime ldt -> ldt.atZone(ZoneOffset.UTC).toInstant();
            case LocalDate ld -> ld.atStartOfDay(ZoneOffset.UTC).toInstant();
            case Date date -> date.toInstant();
            case Calendar cal -> cal.toInstant();
            case Number num -> parseEpoch(num.longValue());
            case CharSequence str -> parseString(str.toString().trim());

            default -> throw new IllegalArgumentException("Unsupported date type: " + input.getClass());
        };
    }

    private static Instant parseEpoch(long epoch) {
        return epoch < 10_000_000_000L ?
            Instant.ofEpochSecond(epoch) :
            Instant.ofEpochMilli(epoch);
    }

    private static Instant parseString(String str) {
        if (str.isEmpty()) {
            throw new IllegalArgumentException("Empty date string");
        }

        if (NUMERIC_PATTERN.matcher(str).matches()) {
            return parseEpoch(Long.parseLong(str));
        }

        try {
            return Instant.parse(str);
        } catch (DateTimeException ignored) {}

        if (NEEDS_TIMEZONE.matcher(str).matches()) {
            try {
                return Instant.parse(str + "Z");
            } catch (DateTimeException ignored) {}
        }

        for (var formatter : FORMATTERS) {
            try {
                var temporal = formatter.withZone(ZoneOffset.UTC).parse(str);
                return convertTemporal(temporal);
            } catch (DateTimeParseException ignored) {}
        }

        throw new IllegalArgumentException("Unparseable date string: " + str);
    }

    private static Instant convertTemporal(TemporalAccessor temporal) {
        return switch (temporal) {
            case Instant i -> i;
            case ZonedDateTime z -> z.toInstant();
            case OffsetDateTime o -> o.toInstant();
            case LocalDateTime l -> l.atZone(ZoneOffset.UTC).toInstant();
            case LocalDate d -> d.atStartOfDay(ZoneOffset.UTC).toInstant();
            default -> throw new DateTimeException("Unsupported temporal type");
        };
    }
}
