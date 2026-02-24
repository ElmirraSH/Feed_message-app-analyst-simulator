*Retention and churn

SELECT toMonday(toDateTime(event_week)) AS __timestamp,
       status AS status,
       sum(user_count) AS "Users"
FROM
  (WITH base_data AS
     (SELECT user_id,
             groupUniqArray(toStartOfWeek(toDate(time), 1)) AS weeks_visited
      FROM simulator_20251220.feed_actions
      GROUP BY user_id) SELECT *
   FROM
     (SELECT this_week AS event_week,
             if(has(weeks_visited, addWeeks(this_week, -1)), 'retained', 'new') AS status,
             toInt64(count(DISTINCT user_id)) AS user_count
      FROM
        (SELECT user_id,
                arrayJoin(weeks_visited) AS this_week,
                weeks_visited
         FROM base_data)
      GROUP BY event_week,
               status
      UNION ALL SELECT addWeeks(previous_week, 1) AS event_week,
                       'gone' AS status,
                       toInt64(-count(DISTINCT user_id)) AS user_count
      FROM
        (SELECT user_id,
                arrayJoin(weeks_visited) AS previous_week,
                weeks_visited
         FROM base_data)
      WHERE NOT has(weeks_visited, addWeeks(previous_week, 1))
      GROUP BY event_week,
               status)
   ORDER BY event_week,
            status) AS virtual_table
GROUP BY status,
         toMonday(toDateTime(event_week))
ORDER BY "Users" ASC
LIMIT 1000;


*WAU news feed and messages app

SELECT toMonday(toDateTime(dt)) AS __timestamp,
       countIf(user_id, used_feed = 1
               and used_message = 1) AS "Лента и новости"
FROM
  (SELECT dt,
          user_id,
          gender,
          age,
          country,
          city,
          os,
          source,
          max(used_feed) AS used_feed,
          max(used_message) AS used_message
   FROM
     (SELECT toDate(time) AS dt,
             user_id,
             gender,
             age,
             country,
             city,
             os,
             source,
             1 AS used_feed,
             0 AS used_message
      FROM simulator_20251220.feed_actions
      UNION ALL SELECT toDate(time) AS dt,
                       user_id,
                       gender,
                       age,
                       country,
                       city,
                       os,
                       source,
                       0 AS used_feed,
                       1 AS used_message
      FROM simulator_20251220.message_actions
      UNION ALL SELECT toDate(time) AS dt,
                       receiver_id AS user_id,
                       gender,
                       age,
                       country,
                       city,
                       os,
                       source,
                       0 AS used_feed,
                       1 AS used_message
      FROM simulator_20251220.message_actions)
   GROUP BY dt,
            user_id,
            gender,
            age,
            country,
            city,
            os,
            source) AS virtual_table
WHERE dt >= toDate('2026-01-01')
  AND dt < toDate('2026-02-01')
GROUP BY toMonday(toDateTime(dt))
ORDER BY "Лента и новости" DESC
LIMIT 50000;