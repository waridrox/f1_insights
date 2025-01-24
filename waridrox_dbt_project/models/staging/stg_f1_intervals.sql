SELECT 
    session_key,
    driver_number,
    date,
    NULLIF(gap_to_leader, 'None') as gap_to_leader,
    NULLIF(interval, 'None') as gap_to_next
FROM {{ source('WARIDROX', 'F1_INTERVALS') }}