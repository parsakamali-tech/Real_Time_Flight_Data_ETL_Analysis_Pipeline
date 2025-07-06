--What percentage of flights are in the "active" status?
SELECT 
	(SUM(CASE WHEN flight_status = 'active' THEN 1 ELSE 0 END)*100)/COUNT(*) AS percentage_status
FROM flights;
--Which departure airports (departure_airport) have the highest average delay (departure_delay)?
SELECT
	ap.name AS departure_airport,
	AVG(dd.delay) AS avg_delay
FROM airports ap
JOIN flights f ON f.departure_airport_id = ap.id
JOIN departure_detail dd ON dd.flight_id = f.id
GROUP BY ap.name
HAVING 
	AVG(dd.delay) > 0
ORDER BY AVG(dd.delay) DESC
LIMIT 10;
--Which airports have the highest average flight duration (flight_duration)?
SELECT 
	ap.name AS airport,
	AVG(f.flight_duration) AS avg_duration
FROM airports ap
JOIN flights f ON f.departure_airport_id = ap.id
GROUP BY ap.name
HAVING 
	AVG(f.flight_duration)>0
ORDER BY avg_duration DESC
LIMIT 10;
--Flight delays at different hours
SELECT 
	EXTRACT(HOUR FROM actual) AS hours,
	AVG(delay) AS avg_delay
FROM departure_detail
GROUP BY hours
HAVING
	EXTRACT(HOUR FROM actual) IS NOT NULL
--	EXTRACT(HOUR FROM actual) BETWEEN 00 AND 12 --if we want morning flights
ORDER BY avg_delay;
--The largest number of flights from the departure airport with IATA code
SELECT
	ap.iata_code AS iata,
	SUM(CASE WHEN f.flight_iata IS NOT NULL THEN 1 ELSE 0 END) AS flight_count
FROM airports ap
JOIN flights f ON ap.id = f.departure_airport_id
GROUP BY ap.iata_code
ORDER BY flight_count DESC
LIMIT 10;