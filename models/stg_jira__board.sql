with base as (

    select *
    from {{ ref('stg_jira__board_tmp') }} 

),

fields as (

    select 

        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_jira__board_tmp')),
                staging_columns=get_board_columns()
            )
        }}

    from base
),

final as (

    select 
        id as board_id,
        name as board_name,
        type as board_type,
        _fivetran_synced
    
    from fields
)

select * from final