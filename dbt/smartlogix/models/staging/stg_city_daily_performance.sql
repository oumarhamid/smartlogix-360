select
    md5(
        concat_ws(
            '||',
            delivery_date::text,
            region_id::text,
            coalesce(trim(city), '')
        )
    ) as city_daily_key,

    delivery_date,
    region_id,
    trim(city) as city,

    orders_total,
    orders_valid_duration,
    orders_within_sla,
    orders_late,
    orders_quality_warning,
    orders_gps_complete,

    unique_couriers,
    unique_aois,

    avg_duration_minutes,
    median_duration_minutes,
    min_duration_minutes,
    max_duration_minutes,

    first_accept_timestamp,
    last_delivery_timestamp,

    sla_compliance_rate,
    gps_completeness_rate,
    quality_warning_rate,
    sla_minutes,

    _gold_processed_at,
    _gold_version

from {{ source('analytics', 'city_daily_performance') }}