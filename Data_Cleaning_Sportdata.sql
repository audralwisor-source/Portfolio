-- Data Cleaning


-- Removing Duplicates


SELECT *
FROM athlete_events_raw;


CREATE TABLE athlete_events_staging
LIKE athlete_events_raw;


INSERT athlete_events_staging
SELECT *
FROM athlete_events_raw;


SELECT *
FROM athlete_events_staging;


SELECT *,
ROW_NUMBER() OVER(
PARTITION BY ID, `Name`, Sex, Age, Height, Weight, Team, NOC, Games, `Year`, Season, City, Sport, `Event`, Medal) AS row_num
FROM athlete_events_staging;


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY ID, `Name`, Sex, Age, Height, Weight, Team, NOC, Games, `Year`, Season, City, Sport, `Event`, Medal) AS row_num
FROM athlete_events_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


CREATE TABLE `athlete_events_staging2` (
  `ID` int DEFAULT NULL,
  `Name` text,
  `Sex` text,
  `Age` int DEFAULT NULL,
  `Height` text,
  `Weight` text,
  `Team` text,
  `NOC` text,
  `Games` text,
  `Year` int DEFAULT NULL,
  `Season` text,
  `City` text,
  `Sport` text,
  `Event` text,
  `Medal` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM athlete_events_staging2;


INSERT INTO athlete_events_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY ID, `Name`, Sex, Age, Height, Weight, Team, NOC, Games, `Year`, Season, City, Sport, `Event`, Medal) AS row_num
FROM athlete_events_staging;


DELETE
FROM athlete_events_staging2
WHERE row_num > 1;



-- Standardizing


SELECT *
FROM athlete_events_staging2;


SELECT DISTINCT Sex
FROM athlete_events_staging2
ORDER BY 1;


SELECT DISTINCT Age
FROM athlete_events_staging2
ORDER BY 1;


SELECT DISTINCT Height
FROM athlete_events_staging2
ORDER BY 1;


SELECT DISTINCT Weight
FROM athlete_events_staging2
ORDER BY 1;


SELECT DISTINCT Weight, ROUND(Weight, 1)
FROM athlete_events_staging2
ORDER BY 1;


UPDATE athlete_events_staging2
SET Weight = ROUND(Weight, 1);


SELECT DISTINCT Team
FROM athlete_events_staging2
ORDER BY 1;


SELECT DISTINCT NOC
FROM athlete_events_staging2
ORDER BY 1;


SELECT DISTINCT Games
FROM athlete_events_staging2
ORDER BY 1;


SELECT DISTINCT `Year`
FROM athlete_events_staging2
ORDER BY 1;


SELECT DISTINCT Season
FROM athlete_events_staging2
ORDER BY 1;


SELECT DISTINCT City
FROM athlete_events_staging2
ORDER BY 1;

SELECT DISTINCT Sport
FROM athlete_events_staging2
ORDER BY 1;


SELECT DISTINCT Medal
FROM athlete_events_staging2
ORDER BY 1;


SELECT DISTINCT `Event`
FROM athlete_events_staging2
ORDER BY 1;


-- Null Values or Blank Values
-- I checked all the columns for Nulls, but found none.
-- No blanks, and none of the 0s or N/As could be updated based on the data I was provided.


ALTER TABLE athlete_events_staging2
DROP COLUMN row_num;