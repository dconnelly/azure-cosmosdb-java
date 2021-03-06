/*
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.microsoft.azure.cosmosdb;

import org.apache.commons.lang3.StringUtils;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

class QueryMetricsUtils {
    static final String Indent = StringUtils.SPACE;
    private static final int NANOS_TO_MILLIS = 1000000;
    private static final String BytesUnitString = "bytes";

    static HashMap<String, Double> parseDelimitedString(String delimitedString) {
        if (delimitedString == null) {
            throw new NullPointerException("delimitedString");
        }

        HashMap<String, Double> metrics = new HashMap<>();

        final int key = 0;
        final int value = 1;
        String[] headerAttributes = StringUtils.split(delimitedString, ";");

        for (String attribute : headerAttributes) {
            String[] attributeKeyValue = StringUtils.split(attribute, "=");

            if (attributeKeyValue.length != 2) {
                throw new NullPointerException("recieved a malformed delimited String");
            }

            String attributeKey = attributeKeyValue[key];
            double attributeValue = Double.parseDouble(attributeKeyValue[value]);
            metrics.put(attributeKey, attributeValue);
        }

        return metrics;
    }

    static Duration durationFromMetrics(HashMap<String, Double> metrics, String key) {
        // Just attempt to get the metrics
        Double durationInMilliseconds = metrics.get(key);
        if (durationInMilliseconds == null) {
            return Duration.ZERO;
        }

        long seconds = (long) (durationInMilliseconds / 1e3);
        long nanoseconds = (long) ((durationInMilliseconds - (seconds * 1e3)) * 1e6);

        return Duration.ofSeconds(seconds, nanoseconds);
    }

    static Duration getDurationFromMetrics(HashMap<String, Double> metrics, String key) {
        double timeSpanInMilliseconds;
        Duration timeSpanFromMetrics;
        timeSpanInMilliseconds = metrics.get(key);
        timeSpanFromMetrics = QueryMetricsUtils.doubleMillisecondsToDuration(timeSpanInMilliseconds);
        return timeSpanFromMetrics;
    }

    private static Duration doubleMillisecondsToDuration(double timeSpanInMilliseconds) {
        long timeInNanoSeconds = (long) (timeSpanInMilliseconds * NANOS_TO_MILLIS);
        return Duration.ofNanos(timeInNanoSeconds);
    }

    private static void appendToStringBuilder(StringBuilder stringBuilder, String property, String value,
                                              String units, int indentLevel) {
        final String FormatString = "%-40s : %15s %-12s %s";

        stringBuilder.append(String.format(
                Locale.ROOT,
                FormatString,
                StringUtils.repeat(Indent, indentLevel) + property,
                value,
                units,
                System.lineSeparator()));
    }

    static void appendBytesToStringBuilder(StringBuilder stringBuilder, String property, long bytes, int indentLevel) {
        final String BytesFormatString = "%d";

        QueryMetricsUtils.appendToStringBuilder(
                stringBuilder,
                property,
                String.format(BytesFormatString, bytes),
                BytesUnitString,
                indentLevel);
    }

    static void appendMillisecondsToStringBuilder(StringBuilder stringBuilder, String property, double milliseconds,
                                                  int indentLevel) {
        final String MillisecondsFormatString = "%f";
        final String MillisecondsUnitString = "milliseconds";

        QueryMetricsUtils.appendToStringBuilder(stringBuilder, property, String.format(MillisecondsFormatString,
                milliseconds), MillisecondsUnitString, indentLevel);
    }

    static void appendNanosecondsToStringBuilder(StringBuilder stringBuilder, String property, double nanoSeconds,
                                                 int indentLevel) {
        final String MillisecondsFormatString = "%.2f";
        final String MillisecondsUnitString = "milliseconds";
        QueryMetricsUtils.appendToStringBuilder(stringBuilder, property, String.format(MillisecondsFormatString,
                nanosToMilliSeconds(nanoSeconds)), MillisecondsUnitString, indentLevel);
    }

    static double nanosToMilliSeconds(double nanos) {
        return nanos / NANOS_TO_MILLIS;
    }

    static void appendHeaderToStringBuilder(StringBuilder stringBuilder, String headerTitle, int indentLevel) {
        final String FormatString = "%s %s";
        stringBuilder.append(String.format(
                Locale.ROOT,
                FormatString,
                String.join(StringUtils.repeat(Indent, indentLevel)) + headerTitle,
                System.lineSeparator()));
    }

    static void appendRUToStringBuilder(StringBuilder stringBuilder, String property, double requestCharge,
                                        int indentLevel) {
        final String RequestChargeFormatString = "%s";
        final String RequestChargeUnitString = "RUs";

        QueryMetricsUtils.appendToStringBuilder(
                stringBuilder,
                property,
                String.format(Locale.ROOT, RequestChargeFormatString, requestCharge),
                RequestChargeUnitString,
                indentLevel);
    }

    static void appendActivityIdsToStringBuilder(StringBuilder stringBuilder, String activityIdsLabel,
                                                 List<String> activityIds, int indentLevel) {
        stringBuilder.append(activityIdsLabel);
        stringBuilder.append(System.lineSeparator());
        for (String activityId : activityIds) {
            stringBuilder.append(Indent);
            stringBuilder.append(activityId);
            stringBuilder.append(System.lineSeparator());
        }
    }

    static void appendPercentageToStringBuilder(StringBuilder stringBuilder, String property, double percentage,
                                                int indentLevel) {
        final String PercentageFormatString = "%.2f";
        final String PercentageUnitString = "%";

        QueryMetricsUtils.appendToStringBuilder(stringBuilder, property, String.format(PercentageFormatString,
                percentage * 100), PercentageUnitString, indentLevel);
    }

    static void appendCountToStringBuilder(StringBuilder stringBuilder, String property, long count, int indentLevel) {
        final String CountFormatString = "%s";
        final String CountUnitString = "";

        QueryMetricsUtils.appendToStringBuilder(
                stringBuilder,
                property,
                String.format(CountFormatString, count),
                CountUnitString,
                indentLevel);
    }

    static void appendNewlineToStringBuilder(StringBuilder stringBuilder) {
        QueryMetricsUtils.appendHeaderToStringBuilder(stringBuilder, StringUtils.EMPTY, 0);
    }
}
