{% snapshot snapshot_users_scd2 %}

{{
  config(
    target_schema = 'snapshots',
    unique_key    = 'user_id',
    strategy      = 'timestamp',
    updated_at    = 'event_ts',
    invalidate_hard_deletes = true
  )
}}

select
    *
from {{ ref('stg_app_users') }}

{% endsnapshot %}
