select
    order_id,
    delivery_date,
    delivery_year,
    delivery_month,
    delivery_day,
    delivery_weekday,
    is_weekend,

    region_id,
    trim(city) as city,
    courier_id,
    aoi_id,
    aoi_type,

    accept_timestamp,
    delivery_timestamp,
    accept_hour,
    delivery_hour,

    delivery_duration_minutes,
    delivery_duration_hours,
    kpi_duration_minutes,
    delivery_duration_status,

    is_valid_duration,
    is_within_sla,
    is_late_delivery,
    sla_minutes,

    accept_gps_lng,
    accept_gps_lat,
    delivery_gps_lng,
    delivery_gps_lat,

    accept_gps_valid,
    delivery_gps_valid,
    has_complete_gps,
    gps_quality_status,

    is_quality_warning,
    quality_warning_count,
    delivery_count,

    source_file,
    source_sha256,
    dataset_revision,
    source_silver_version,

    _gold_processed_at,
    _gold_version

from {{ source('analytics', 'delivery_fact') }}