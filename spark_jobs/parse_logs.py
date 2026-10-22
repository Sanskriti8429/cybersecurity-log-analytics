from pyspark.sql import functions as F

AUTH_PATTERN = (
    r"^(\w{3}\s+\d+\s+\d{2}:\d{2}:\d{2}) (\S+) sshd\[(\d+)\]: "
    r"(Accepted|Failed) password for (\S+) from (\S+) port (\d+)"
)


def parse_auth(df):
    """Turn raw log lines (column 'value') into structured columns."""
    time_str = F.regexp_replace(F.regexp_extract("value", AUTH_PATTERN, 1), r"\s+", " ")
    parsed = df.select(
        "event_id",
        time_str.alias("time_str"),
        F.regexp_extract("value", AUTH_PATTERN, 4).alias("outcome"),
        F.regexp_extract("value", AUTH_PATTERN, 5).alias("user"),
        F.regexp_extract("value", AUTH_PATTERN, 6).alias("src_ip"),
        F.regexp_extract("value", AUTH_PATTERN, 7).cast("int").alias("port"),
        "kafka_ts",
    )
    ts = F.to_timestamp(
        F.concat_ws(" ", F.year("kafka_ts").cast("string"), "time_str"),
        "yyyy MMM d HH:mm:ss",
    )
    return parsed.withColumn("failed", (F.col("outcome") == "Failed").cast("int")).withColumn("ts", ts)

FW_PATTERN = (
    r"^(\w{3}\s+\d+\s+\d{2}:\d{2}:\d{2}) (\S+) kernel: \[UFW (ALLOW|BLOCK)\] "
    r"IN=\S* OUT=\S* SRC=(\S+) DST=(\S+) .*?SPT=(\d+) DPT=(\d+)"
)


def parse_firewall(df):
    """Turn raw firewall lines (column 'value') into structured columns."""
    time_str = F.regexp_replace(F.regexp_extract("value", FW_PATTERN, 1), r"\s+", " ")
    parsed = df.select(
        "event_id",
        time_str.alias("time_str"),
        F.regexp_extract("value", FW_PATTERN, 3).alias("action"),
        F.regexp_extract("value", FW_PATTERN, 4).alias("src_ip"),
        F.regexp_extract("value", FW_PATTERN, 6).cast("int").alias("src_port"),
        F.regexp_extract("value", FW_PATTERN, 7).cast("int").alias("dst_port"),
        "kafka_ts",
    )
    ts = F.to_timestamp(
        F.concat_ws(" ", F.year("kafka_ts").cast("string"), "time_str"),
        "yyyy MMM d HH:mm:ss",
    )
    return parsed.withColumn("ts", ts)


def detect_port_scans(parsed, threshold=15):
    """Return the distinct src_ips that hit >= threshold distinct ports in a 1-minute window."""
    return (
        parsed.groupBy(F.window("ts", "1 minute", "10 seconds"), "src_ip")
        .agg(F.countDistinct("dst_port").alias("distinct_ports"))
        .filter(F.col("distinct_ports") >= threshold)
        .select("src_ip")
        .distinct()
    )