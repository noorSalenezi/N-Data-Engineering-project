-- Payment rows that break the provider's contract. Kept with the reason instead of reaching revenue.
-- Owner: payments-provider-integration (see the contract). The pipeline stops if this exceeds 1% of the feed.
select
    *,
    case
        when amount is null or amount <= 0                          then 'non-positive amount'
        when currency is distinct from 'SAR'                        then 'unexpected currency'
        when status not in ('captured', 'failed', 'refunded')       then 'unknown status'
        when method not in ('card', 'apple_pay', 'wallet')          then 'unknown method'
        else 'null in a required field'
    end                                                             as reject_reason
from {{ source('raw', 'payments') }}
where not coalesce({{ payment_is_valid() }}, false)
