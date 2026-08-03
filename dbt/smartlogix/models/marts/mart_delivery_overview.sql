select
    md5(
        concat_ws(
            '||',
            delivery_date::text,
            region_id::text,
            city
        )
    ) as delivery_overview_key,

    delivery_date,
    region_id,
    city,

    sum(delivery_count) as orders_total,
    count(*) filter (
        where is_valid_duration
    ) as orders_valid_duration,
    count(*) filter (
        where is_within_sla
    ) as orders_within_sla,
    count(*) filter (
        where is_late_delivery
    ) as orders_late,
    count(*) filter (
        where has_complete_gps
    ) as orders_gps_complete,
    count(*) filter (
        where is_quality_warning
    ) as orders_quality_warning,

    round(
        avg(delivery_duration_minutes)
        filter (where is_valid_duration)::numeric,
        2
    ) as avg_duration_minutes,

    round(
        100.0
        * count(*) filter (where is_within_sla)
        / nullif(
            count(*) filter (where is_valid_duration),
            0
        ),
        2
    ) as sla_compliance_rate,

    round(
        100.0
        * count(*) filter (where has_complete_gps)
        / nullif(count(*), 0),
        2
    ) as gps_completeness_rate,

    round(
        100.0
        * count(*) filter (where is_quality_warning)
        / nullif(count(*), 0),
        2
    ) as quality_warning_rate

from {{ ref('stg_delivery_fact') }}

group by
    delivery_date,
    region_id,
    city