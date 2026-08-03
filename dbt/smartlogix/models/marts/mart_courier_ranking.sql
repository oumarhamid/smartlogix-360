with courier_performance as (

    select
        region_id,
        city,
        courier_id,

        sum(orders_total) as orders_total,
        sum(orders_valid_duration) as orders_valid_duration,
        sum(orders_within_sla) as orders_within_sla,
        sum(orders_late) as orders_late,
        sum(orders_gps_complete) as orders_gps_complete,
        sum(orders_quality_warning) as orders_quality_warning,

        round(
            (
                sum(
                    avg_duration_minutes
                    * orders_valid_duration
                )
                / nullif(
                    sum(orders_valid_duration),
                    0
                )
            )::numeric,
            2
        ) as avg_duration_minutes,

        round(
            100.0
            * sum(orders_within_sla)
            / nullif(
                sum(orders_valid_duration),
                0
            ),
            2
        ) as sla_compliance_rate,

        round(
            100.0
            * sum(orders_gps_complete)
            / nullif(sum(orders_total), 0),
            2
        ) as gps_completeness_rate,

        round(
            100.0
            * sum(orders_quality_warning)
            / nullif(sum(orders_total), 0),
            2
        ) as quality_warning_rate

    from {{ ref('stg_courier_daily_performance') }}

    group by
        region_id,
        city,
        courier_id
)

select
    md5(
        concat_ws(
            '||',
            region_id::text,
            city,
            courier_id::text
        )
    ) as courier_ranking_key,

    *,

    dense_rank() over (
        partition by region_id, city
        order by
            sla_compliance_rate desc,
            orders_total desc,
            avg_duration_minutes asc
    ) as city_rank

from courier_performance