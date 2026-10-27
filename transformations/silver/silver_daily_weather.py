from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="silver_daily_weather",
    comment=(
        "Silver: cleansed daily weather observations — implausible and null-keyed "
        "rows dropped, observation year/month derived for analytics."
    ),
    cluster_by=["station_id", "date"],
)
@dp.expect_all_or_drop({
    "valid_station_id": "station_id IS NOT NULL",
    "valid_date": "date IS NOT NULL AND date BETWEEN DATE'1950-01-01' AND DATE'2026-12-31'",
    "valid_tmax": "tmax_c IS NULL OR tmax_c BETWEEN -80 AND 60",
    "valid_tmin": "tmin_c IS NULL OR tmin_c BETWEEN -80 AND 60",
    "valid_tavg": "tavg_c IS NULL OR tavg_c BETWEEN -80 AND 60",
    "valid_prcp": "prcp_mm IS NULL OR prcp_mm >= 0",
    "valid_snow": "snow_mm IS NULL OR snow_mm >= 0",
    "valid_snwd": "snwd_mm IS NULL OR snwd_mm >= 0",
})
@dp.expect("no_qa_flagged_elements", "qflag_count = 0")
def silver_daily_weather():
    """Clean the bronze observations incrementally.

    The source guarantees one row per station-date; range expectations drop
    only physically implausible values (the GHCN -9999 sentinel is excluded
    upstream, this is a defensive check). Rows with failed NCEI QA flags
    (qflag_count > 0) are kept but tracked via the warn-level expectation.
    """
    return (
        spark.readStream.table("bronze_daily_weather")
        .withColumn("year", F.year("date").cast("int"))
        .withColumn("month", F.month("date").cast("int"))
    )
