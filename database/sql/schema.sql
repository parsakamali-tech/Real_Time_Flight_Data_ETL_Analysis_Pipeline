CREATE TABLE airlines (
"id" SERIAL PRIMARY KEY,
"name" TEXT,
"iata_code" VARCHAR(3),
"icao_code" VARCHAR(4)
);
CREATE TABLE airports (
"id" SERIAL PRIMARY KEY,
"name" TEXT,
"iata_code" VARCHAR(3),
"icao_code" VARCHAR(4),
"timezone" TEXT
);
CREATE TABLE flights (
"id" SERIAL PRIMARY KEY,
"flight_date" DATE,
"flight_status" TEXT,
"airline_id" INTEGER REFERENCES airlines("id"),
"departure_airport_id" INTEGER REFERENCES airports("id"),
"arrival_airport_id" INTEGER REFERENCES airports("id"),
"flight_number" TEXT,
"flight_iata" TEXT,
"flight_icao" TEXT,
"flight_duration" INTEGER,
"delay_outlier" BOOLEAN,
"flight_duration_outlier" BOOLEAN
);
CREATE TABLE departure_detail (
"flight_id" SERIAL PRIMARY KEY REFERENCES flights("id"),
"gate" TEXT,
"terminal" TEXT,
"delay" INTEGER,
"schaduled" TIMESTAMP WITH TIME ZONE,
"estimated" TIMESTAMP WITH TIME ZONE,
"actual" TIMESTAMP WITH TIME ZONE,
"estimated_runway" TIMESTAMP WITH TIME ZONE,
"actual_runway" TIMESTAMP WITH TIME ZONE
);
CREATE TABLE arrival_detail (
"flight_id" SERIAL PRIMARY KEY REFERENCES flights("id"),
"gate" TEXT,
"terminal" TEXT,
"baggage" TEXT,
"schaduled" TIMESTAMP WITH TIME ZONE
);