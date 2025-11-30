-- Create project_capstone DB
CREATE DATABASE nyc;

-- Create user for project_capstone
CREATE USER capstone_user WITH ENCRYPTED PASSWORD 'capstone_password';
GRANT ALL PRIVILEGES ON DATABASE nyc TO capstone_user;

CREATE TABLE green_taxi_trip (
    vendor_id BIGINT,
    pickup_datetime VARCHAR(19),
    dropoff_datetime VARCHAR(19),
    store_and_fwd_flag VARCHAR(3),              -- sebelumnya VARCHAR(1), overflow karena nilai 3 karakter
    rate_code_id NUMERIC(3,1),                  -- sebelumnya NUMERIC(2,1), kolom hilang tapi disiapkan
    pickup_location_id BIGINT,                  -- kolom hilang tapi disiapkan
    dropoff_location_id BIGINT,                 -- kolom hilang tapi disiapkan
    passenger_count NUMERIC(3,1),               -- ditingkatkan dari 2,1 → jaga-jaga
    trip_distance NUMERIC(10,2),                -- sebelumnya 8,2 → overflow karena nilai 41305.67
    fare_amount NUMERIC(6,2),                   -- tetap, karena 603.6 masih aman
    extra NUMERIC(5,2),                         -- ditingkatkan dari 4,2
    mta_tax NUMERIC(4,2),                       -- ditingkatkan dari 3,1
    tip_amount NUMERIC(6,2),                    -- ditingkatkan dari 5,2
    tolls_amount NUMERIC(5,2),                  -- ditingkatkan dari 4,2
    ehail_fee TEXT,
    improvement_surcharge NUMERIC(4,2),         -- ditingkatkan dari 3,1
    total_amount NUMERIC(7,2),                  -- ditingkatkan dari 6,2
    payment_type VARCHAR(20),                   -- sebelumnya NUMERIC(2,1), tapi audit bilang VARCHAR lebih cocok
    trip_type NUMERIC(3,1),                     -- ditingkatkan dari 2,1
    congestion_surcharge NUMERIC(5,2),          -- ditingkatkan dari 4,2
    cbd_congestion_fee NUMERIC(4,2),            -- ditingkatkan dari 3,2
    run_date VARCHAR(10)
);

ALTER TABLE green_taxi_trip
ADD CONSTRAINT green_taxi_unique UNIQUE (
    vendor_id,
	pickup_datetime,
	dropoff_datetime,
	pickup_location_id,
	dropoff_location_id,
	passenger_count,
	trip_distance
);

ALTER TABLE green_taxi_trip
ALTER COLUMN fare_amount TYPE NUMERIC(5,2);


