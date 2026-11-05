-- Calendar dimension. The Saudi weekend is Friday + Saturday.
with days as (
    select unnest(range(date '2026-01-01', date '2027-01-01', interval 1 day))::date as date_day
)

select
    date_day,
    year(date_day)                                      as year,
    month(date_day)                                     as month,
    day(date_day)                                       as day_of_month,
    weekofyear(date_day)                                as iso_week,
    dayname(date_day)                                   as day_name,
    ['الأحد', 'الاثنين', 'الثلاثاء', 'الأربعاء', 'الخميس', 'الجمعة', 'السبت'][dayofweek(date_day) + 1] as day_name_ar,
    dayofweek(date_day) in (5, 6)                       as is_weekend,
    date_day = date '2026-09-23'                        as is_national_day
from days
