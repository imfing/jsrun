"""
Temporal <-> Python conversion tests.

JavaScript Temporal values cross into Python as datetime types and
Python date/time/timedelta values cross into JavaScript as Temporal
instances, via the tagged-object bridge in `src/runtime/ops.rs` and the
conversions in `src/runtime/conversion.rs`.
"""

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from jsrun import Runtime


class TestTemporalToPython:
    def test_instant_to_aware_datetime(self):
        with Runtime() as rt:
            result = rt.eval("Temporal.Instant.from('2026-10-03T12:30:45.123456789Z')")
            assert isinstance(result, datetime)
            assert result.tzinfo is not None
            assert result.utcoffset() == timedelta(0)
            # Sub-microsecond precision is truncated.
            assert result == datetime(
                2026, 10, 3, 12, 30, 45, 123456, tzinfo=timezone.utc
            )

    def test_negative_epoch_instant(self):
        with Runtime() as rt:
            result = rt.eval("Temporal.Instant.from('1960-01-01T00:00:00.5Z')")
            assert result == datetime(1960, 1, 1, 0, 0, 0, 500000, tzinfo=timezone.utc)

    def test_zoned_datetime_iana(self):
        with Runtime() as rt:
            result = rt.eval(
                "Temporal.ZonedDateTime.from('2026-07-04T09:00:00[America/New_York]')"
            )
            assert isinstance(result, datetime)
            assert result.tzinfo == ZoneInfo("America/New_York")
            assert result.hour == 9
            assert result.utcoffset() == timedelta(hours=-4)  # EDT

    def test_zoned_datetime_offset_zone(self):
        with Runtime() as rt:
            result = rt.eval(
                "Temporal.ZonedDateTime.from('2026-10-03T10:00:00+05:30[+05:30]')"
            )
            assert result.utcoffset() == timedelta(hours=5, minutes=30)
            assert result.hour == 10

    def test_plain_date(self):
        with Runtime() as rt:
            assert rt.eval("Temporal.PlainDate.from('2026-10-03')") == date(2026, 10, 3)

    def test_plain_date_non_iso_calendar_normalized(self):
        with Runtime() as rt:
            result = rt.eval(
                "Temporal.PlainDate.from('2026-10-03').withCalendar('hebrew')"
            )
            assert result == date(2026, 10, 3)

    def test_plain_time(self):
        with Runtime() as rt:
            result = rt.eval("Temporal.PlainTime.from('14:15:16.123456789')")
            assert result == time(14, 15, 16, 123456)

    def test_plain_datetime_naive(self):
        with Runtime() as rt:
            result = rt.eval("Temporal.PlainDateTime.from('2026-10-03T14:15:16.5')")
            # PlainDateTime is zoneless wall-clock time; a naive datetime is
            # exactly the expected value.
            assert result == datetime(2026, 10, 3, 14, 15, 16, 500000)  # noqa: DTZ001
            assert result.tzinfo is None

    def test_duration(self):
        with Runtime() as rt:
            assert rt.eval("Temporal.Duration.from('PT1H30M')") == timedelta(
                hours=1, minutes=30
            )
            assert rt.eval("Temporal.Duration.from('-PT2.5S')") == timedelta(
                seconds=-2.5
            )
            assert rt.eval("Temporal.Duration.from({ days: 3 })") == timedelta(days=3)

    def test_large_duration(self):
        with Runtime() as rt:
            rt.bind_function("echo", lambda v: v)
            big = timedelta(days=106751992)
            assert rt.eval(
                f"echo(Temporal.Duration.from({{ days: {big.days} }})) !== null"
            )
            check = rt.eval("(d) => d")
            assert check(big) == big

    def test_zoned_datetime_at_year_boundaries(self):
        with Runtime() as rt:
            low = rt.eval(
                "Temporal.ZonedDateTime.from('0001-01-01T00:00:00+01:00[+01:00]')"
            )
            assert low.year == 1
            assert low.utcoffset() == timedelta(hours=1)
            high = rt.eval(
                "Temporal.ZonedDateTime.from('9999-12-31T23:59:59-05:00[-05:00]')"
            )
            assert high.year == 9999
            assert high.utcoffset() == timedelta(hours=-5)

    def test_zoned_datetime_dst_fold(self):
        with Runtime() as rt:
            # 2026-11-01 in America/New_York: 01:30 EDT and 01:30 EST both
            # exist; the two instants must stay one hour apart after
            # conversion, with the second occurrence carrying fold=1.
            first = rt.eval(
                "Temporal.Instant.from('2026-11-01T05:30:00Z')"
                ".toZonedDateTimeISO('America/New_York')"
            )
            second = rt.eval(
                "Temporal.Instant.from('2026-11-01T06:30:00Z')"
                ".toZonedDateTimeISO('America/New_York')"
            )
            assert first.utcoffset() == timedelta(hours=-4)  # EDT
            assert second.utcoffset() == timedelta(hours=-5)  # EST
            assert second.fold == 1
            # Same-zone aware subtraction compares wall clocks (ignoring
            # fold), so compare the actual instants via timestamps.
            assert second.timestamp() - first.timestamp() == 3600

    def test_calendar_relative_duration_not_converted(self):
        with Runtime() as rt:
            # years/months/weeks have no fixed length; stays unconverted.
            result = rt.eval("Temporal.Duration.from({ months: 2 })")
            assert not isinstance(result, timedelta)

    def test_nested_in_containers(self):
        with Runtime() as rt:
            result = rt.eval(
                "({ when: Temporal.PlainDate.from('2026-01-02'),"
                " spans: [Temporal.Duration.from('PT5S')] })"
            )
            assert result == {"when": date(2026, 1, 2), "spans": [timedelta(seconds=5)]}

    def test_ops_path_argument(self):
        with Runtime() as rt:
            seen = {}
            rt.bind_function("capture", lambda v: seen.update(value=v))
            rt.eval("capture(Temporal.PlainDate.from('2026-10-03'))")
            assert seen["value"] == date(2026, 10, 3)


class TestPythonToTemporal:
    def test_date_to_plain_date(self):
        with Runtime() as rt:
            check = rt.eval(
                "(d) => d instanceof Temporal.PlainDate ? d.toString() : 'nope'"
            )
            assert check(date(2026, 10, 3)) == "2026-10-03"

    def test_time_to_plain_time(self):
        with Runtime() as rt:
            check = rt.eval(
                "(t) => t instanceof Temporal.PlainTime ? t.toString() : 'nope'"
            )
            assert check(time(14, 15, 16, 123456)) == "14:15:16.123456"

    def test_timedelta_to_duration(self):
        with Runtime() as rt:
            check = rt.eval(
                "(d) => d instanceof Temporal.Duration ? d.total('milliseconds') : 'nope'"
            )
            assert check(timedelta(hours=1, minutes=30)) == 5_400_000
            assert check(timedelta(seconds=-2.5)) == -2_500

    def test_datetime_still_maps_to_date(self):
        # Back-compat: datetime keeps converting to a JS Date.
        with Runtime() as rt:
            check = rt.eval("(d) => d instanceof Date")
            assert check(datetime(2026, 10, 3, tzinfo=timezone.utc)) is True

    def test_aware_time_rejected(self):
        with Runtime() as rt:
            identity = rt.eval("(t) => t")
            with pytest.raises(Exception, match="tzinfo"):
                identity(time(12, 0, tzinfo=timezone.utc))

    def test_ops_path_return_value(self):
        with Runtime() as rt:
            rt.bind_function("today", lambda: date(2026, 10, 3))
            assert rt.eval("today() instanceof Temporal.PlainDate") is True
            assert rt.eval("today().toString()") == "2026-10-03"

    def test_roundtrip_through_python(self):
        with Runtime() as rt:
            rt.bind_function("echo", lambda v: v)
            assert (
                rt.eval(
                    "echo(Temporal.PlainDate.from('2026-10-03'))"
                    ".equals(Temporal.PlainDate.from('2026-10-03'))"
                )
                is True
            )
            assert (
                rt.eval("echo(Temporal.Duration.from('PT90S')).total('seconds')") == 90
            )
