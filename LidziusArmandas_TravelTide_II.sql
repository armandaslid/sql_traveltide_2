/*
Question #1:
Return users who have booked and completed at least 10 flights, ordered by user_id.

Expected column names: `user_id`
*/

-- Q1 solution:

-- With CTE:

WITH tt AS					-- Temporal table (CTE)
(
SELECT user_id					-- Selecting user_ids
FROM sessions
WHERE flight_booked = 'true'			-- Filtering those who booked and completed (didn't cancel) the flights
AND cancellation = 'false'
)

SELECT user_id
FROM tt
GROUP BY 1
HAVING COUNT(*) >= 10				-- Filtering user_ids with 10 or more flights
ORDER BY 1
;

-- Or without CTE:

SELECT user_id
FROM sessions
WHERE flight_booked = 'true'
AND cancellation = 'false'
GROUP BY 1
HAVING COUNT(user_id) >= 10
ORDER BY 1
;


/*
Question #2: 
Write a solution to report the trip_id of sessions where:

1. session resulted in a booked flight
2. booking occurred in May, 2022
3. booking has the maximum flight discount on that respective day.

If in one day there are multiple such transactions, return all of them.

Expected column names: `trip_id`
*/

-- Q2 solution:

WITH tt AS
(
SELECT  trip_id
	,DENSE_RANK() OVER(PARTITION BY DATE_TRUNC('day', session_start) ORDER BY flight_discount_amount DESC) AS highest_discount
FROM sessions							-- Ranking flight discounts to find the highest
WHERE flight_booked = 'true'					-- Filtering that the flight was booked
AND DATE_TRUNC('day', session_start) BETWEEN '2022-05-01' AND '2022-05-31'	-- Filtering the date to be in May 2022
AND flight_discount_amount IS NOT NULL				-- And that the discount would have a value
)

SELECT trip_id							-- Selecting trip_ids from temporal table with highest discounts
FROM tt
WHERE highest_discount = 1
ORDER BY 1
;


/*
Question #3: 
Write a solution that will, for each user_id of users with greater than 10 flights, 
find out the largest window of days between 
the departure time of a flight and the departure time 
of the next departing flight taken by the user.

Expected column names: `user_id`, `biggest_window`
*/

-- Q3 solution:

WITH tt AS
(
SELECT  sessions.user_id
	,flights.departure_time
	,LAG(flights.departure_time) OVER(PARTITION BY sessions.user_id ORDER BY flights.departure_time) -- LAG Window function, to access data from a previous row in the same result set and then calculating the differences 
FROM sessions
LEFT JOIN flights ON sessions.trip_id = flights.trip_id
WHERE sessions.user_id IN (SELECT user_id
			   FROM sessions
			   WHERE flight_booked = 'true'
			   GROUP BY 1
			   HAVING COUNT(flight_booked) > 10) 	-- Subquery to find user_ids with more than 10 booked flights
)

SELECT  user_id
	,ROUND(EXTRACT(DAY FROM MAX(departure_time - lag))
	+
	EXTRACT(HOURS FROM MAX(departure_time - lag))
	/
	24) AS biggest_window 					-- biggest_window rounded in days (instead of days and hours)
FROM tt
GROUP BY 1
;

/*
Question #4: 
Find the user_id’s of people whose origin airport is Boston (BOS) 
and whose first and last flight were to the same destination. 
Only include people who have flown out of Boston at least twice.

Expected column names: `user_id`
*/

-- Q4 solution:

WITH ranked AS
(
SELECT 	sessions.user_id
	,flights.destination_airport
	,ROW_NUMBER() OVER(PARTITION BY sessions.user_id ORDER BY flights.departure_time ASC) AS first_time -- Window function, to find first and last departure (by ranking first ascending and then descending)
	,ROW_NUMBER() OVER(PARTITION BY sessions.user_id ORDER BY flights.departure_time DESC) AS last_time
FROM sessions
LEFT JOIN flights ON sessions.trip_id=flights.trip_id
WHERE flights.origin_airport = 'BOS'			-- Filtering original airport to be Boston
AND sessions.user_id IN (SELECT sessions.user_id
			FROM sessions
			LEFT JOIN flights ON sessions.trip_id=flights.trip_id
			WHERE flights.origin_airport = 'BOS'
			AND sessions.cancellation = 'false'
			GROUP BY 1
			HAVING COUNT(*) >= 2)		-- Subquery to find user_ids who flown at least 2 times (considering that flight might have been canceled) and original airport was Boston 
)

SELECT r1.user_id
FROM ranked r1
JOIN ranked r2 ON r1.user_id=r2.user_id 		-- Self JOIN the table from CTE, to match first and last destination to be the same, and also to match first and last flights
WHERE r1.first_time=1
AND r2.last_time=1
AND r1.destination_airport=r2.destination_airport
;
