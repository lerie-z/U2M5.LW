select client_surr_id, driver_surr_id, trip_id, avg_revenue from dw_level.fct_orders_mm
    where avg_trip_duration > 200
    model unique single reference
        partition by (client_surr_id)
        dimension by (driver_surr_id)
        measures (client_surr_id as client_id
                 ,driver_surr_id as driver_id, trip_id as trip)
        rules();