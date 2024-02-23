--
-- PART ONE: Create tables for analysis and upload data from csv file
--

DROP TABLE IF EXISTS CovidDeaths;
CREATE TABLE IF NOT EXISTS CovidDeaths (
	iso_code	text,
	continent	text,
	location	text,
	date	date, 
	population	decimal,
	total_cases	decimal,
	new_cases decimal,
	new_cases_smoothed	decimal,
	total_deaths	decimal,
	new_deaths	decimal,
	new_deaths_smoothed	decimal,
	total_cases_per_million	decimal,
	new_cases_per_million	decimal,
	new_cases_smoothed_per_million	decimal,
	total_deaths_per_million	decimal,
	new_deaths_per_million	decimal,
	new_deaths_smoothed_per_million	decimal,
	reproduction_rate	decimal,
	icu_patients	decimal,
	icu_patients_per_million decimal,	
	hosp_patients	decimal,
	hosp_patients_per_million decimal,
	weekly_icu_admissions	decimal,
	weekly_icu_admissions_per_million	decimal,
	weekly_hosp_admissions	decimal,
	weekly_hosp_admissions_per_million decimal
);
DROP TABLE IF EXISTS covidvaccinations;
CREATE TABLE IF NOT EXISTS CovidVaccinations (
	iso_code	text,
	continent	text,
	location	text,
	date	date,
	total_tests	numeric,
	new_tests	numeric,
	total_tests_per_thousand	numeric,
	new_tests_per_thousand	numeric,
	new_tests_smoothed	numeric,
	new_tests_smoothed_per_thousand	numeric,
	positive_rate	numeric,
	tests_per_case	numeric, 
	tests_units	text, 
	total_vaccinations	numeric,
	people_vaccinated	numeric,
	people_fully_vaccinated	numeric, 
	total_boosters	numeric,
	new_vaccinations	numeric,
	new_vaccinations_smoothed	numeric,
	total_vaccinations_per_hundred	numeric,
	people_vaccinated_per_hundred	numeric,
	people_fully_vaccinated_per_hundred	numeric,
	total_boosters_per_hundred	numeric,
	new_vaccinations_smoothed_per_million	numeric,
	new_people_vaccinated_smoothed	numeric,
	new_people_vaccinated_smoothed_per_hundred	numeric, 
	stringency_index	numeric,
	population_density	numeric,
	median_age	numeric,
	aged_65_older	numeric,
	aged_70_older	numeric,
	gdp_per_capita	numeric,
	extreme_poverty	numeric,
	cardiovasc_death_rate	numeric,
	diabetes_prevalence	numeric,
	female_smokers	numeric,
	male_smokers	numeric,
	handwashing_facilities	numeric,
	hospital_beds_per_thousand	numeric,
	life_expectancy	numeric,
	human_development_index	numeric,
	population	numeric,
	excess_mortality_cumulative_absolute	numeric,
	excess_mortality_cumulative numeric,
	excess_mortality	numeric,
	excess_mortality_cumulative_per_million numeric
);

UPDATE coviddeaths
	SET total_cases = CAST(total_cases AS bigint),
		total_deaths = CAST(total_deaths AS bigint),
		population = CAST(population AS bigint);

-- 
-- PART TWO: Analysis
--

--
-- 2a: Analyis The Netherlands
--

-- An overview of total cases and mortalities in the Netherlands
SELECT location, date, population, total_cases, ROUND((total_cases/population)*100, 2) AS Total_infected_perc, ROUND((total_deaths/total_cases)*100, 2) AS Infected_Death_percentage, total_deaths
FROM CovidDeaths
WHERE location = 'Netherlands'
ORDER BY location, date;


-- Aggregate to weekly data to see trends per week
SELECT  EXTRACT(year FROM date) AS year, EXTRACT(week FROM date) AS week, population, CAST(SUM(new_cases) AS bigint) AS total_cases, ROUND((SUM(total_cases)/SUM(population))*100, 2) AS Total_infected_perc, ROUND((SUM(new_deaths)/SUM(new_cases))*100, 2) AS Infected_Death_perc, COALESCE(SUM(total_deaths),0) AS total_deaths
FROM CovidDeaths
WHERE location = 'Netherlands'
GROUP BY year, week, population
HAVING SUM(new_cases) != 0
ORDER BY year, week;

-- 
--  2b: Country-level analysis

-- Top 10 countries with the highest infection rate per year
WITH inf20 AS(
	SELECT location, population, row_number() OVER(ORDER BY MAX(ROUND((total_cases/population), 2))*100 DESC) AS rank20, MAX(total_cases) AS highestInfectionCount, MAX(ROUND((total_cases/population), 2))*100 AS highest_infection_rate_2020
	FROM CovidDeaths
	WHERE total_cases/population IS NOT NULL 
	AND EXTRACT(year FROM date) = 2020
	GROUP BY location, population
	ORDER BY highest_infection_rate_2020 DESC
	LIMIT 10),
		
	inf21 AS(
		SELECT RANK() OVER(ORDER BY location) AS rank21, location, population, MAX(total_cases) AS highestInfectionCount, MAX(ROUND((total_cases/population), 2))*100 AS highest_infection_rate_2021
		FROM CovidDeaths
		WHERE total_cases/population IS NOT NULL 
			AND EXTRACT(year FROM date) = 2021
		GROUP BY location, population
		ORDER BY highest_infection_rate_2021 DESC
		LIMIT 10),
		
	inf22 AS (
		SELECT location, population, MAX(total_cases) AS highestInfectionCount, MAX(ROUND((total_cases/population), 2))*100 AS highest_infection_rate_2022
		FROM CovidDeaths
		WHERE total_cases/population IS NOT NULL 
			AND EXTRACT(year FROM date) = 2022
		GROUP BY location, population
		ORDER BY highest_infection_rate_2022 DESC
		LIMIT 10),
		
	inf23 AS(
		SELECT location, population, MAX(total_cases) AS highestInfectionCount, MAX(ROUND((total_cases/population), 2))*100 AS highest_infection_rate_2023
		FROM CovidDeaths
		WHERE total_cases/population IS NOT NULL 
			AND EXTRACT(year FROM date) = 2023
		GROUP BY location, population
		ORDER BY highest_infection_rate_2023 DESC
		LIMIT 10)

SELECT inf20.location, highest_infection_rate_2020, sub21.location, highest_infection_rate_2021, sub22.location, highest_infection_rate_2022, sub23.location, highest_infection_rate_2023
FROM inf20
LEFT JOIN (SELECT location, highest_infection_rate_2021, row_number() OVER(ORDER BY highest_infection_rate_2021 DESC) AS rank21
FROM inf21) AS sub21
	ON inf20.rank20 = sub21.rank21
LEFT JOIN (SELECT location, highest_infection_rate_2022, row_number() OVER(ORDER BY highest_infection_rate_2022 DESC) AS rank22
FROM inf22) AS sub22
	ON inf20.rank20 = sub22.rank22
LEFT JOIN (SELECT location, highest_infection_rate_2023, row_number() OVER(ORDER BY highest_infection_rate_2023 DESC) AS rank23
FROM inf23) AS sub23
	ON inf20.rank20 = sub23.rank23


-- Which countries had the highest total death count?
SELECT location, MAX(total_deaths) AS Total_death_count
FROM CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location
HAVING MAX(total_deaths) IS NOT NULL
ORDER BY Total_death_count DESC
LIMIT 10;


--
-- 2c: Continent level analysis

-- What are the total death counts per continent?
SELECT location, MAX(total_deaths) AS Total_death_count
FROM CovidDeaths
WHERE continent IS NULL
GROUP BY location
HAVING MAX(total_deaths) IS NOT NULL
ORDER BY Total_death_count DESC;


-- 2d: Global level analysis 

-- Daily global mortality count
SELECT date, CAST(SUM(new_cases) AS bigint) AS total_cases, CAST(SUM(new_deaths) AS bigint) AS total_deaths
FROM Coviddeaths
GROUP BY date
ORDER BY date, total_cases;

-- Daily cumulative global mortality count
SELECT DISTINCT date, CAST(SUM(new_cases) OVER(ORDER BY date) AS bigint) AS total_cases, CAST(SUM(new_deaths) OVER (ORDER BY date) AS bigint) AS total_deaths
FROM coviddeaths
ORDER BY date;

-- Global aggregate values; total infected, total mortalities, total infected to mortality percentage
SELECT  CAST(SUM(new_cases) AS bigint) AS total_cases, CAST(SUM(new_deaths) AS bigint) AS total_deaths, ROUND((SUM(new_deaths)/SUM(new_cases))*100, 2) AS Death_percentage_infected
FROM Coviddeaths
HAVING SUM(new_cases) != 0
ORDER BY total_cases;


--
-- PART 3: Country-level vaccination rate analysis 
--

DROP TABLE IF EXISTS popvactemp;
CREATE TEMP TABLE popvactemp(
	continent varchar(255),
	location varchar (255),
	date date,
	population numeric,
	new_vaccinations numeric,
	running_total_vaccinated numeric
);

INSERT INTO popvactemp (
	SELECT D.continent, D.location, D.date, D.population, vac.new_vaccinations, (SUM(vac.new_vaccinations) OVER(PARTITION BY D.location ORDER BY D.location, D.date))::bigint AS Running_total_vaccinated
	FROM coviddeaths AS D
	LEFT JOIN covidvaccinations AS vac
		ON D.date = vac.date
		AND D.location = vac.location
	WHERE D.continent IS NOT NULL
);

-- Daily vaccination analysis for United states (enter any country)
SELECT *, ROUND((running_total_vaccinated/population)*100,4) AS running_vaccinated_percentage
FROM popvactemp
WHERE location = 'United States'

-- Yearly vaccination analysis all countries.
SELECT location, 
	MAX(CASE WHEN EXTRACT(year FROM date) = 2020 THEN running_vaccinated_percentage END) Vac_perc_2020,
	MAX(CASE WHEN EXTRACT(year FROM date) = 2021 THEN running_vaccinated_percentage END) Vac_perc_2021,
	MAX(CASE WHEN EXTRACT(year FROM date) = 2022 THEN running_vaccinated_percentage END) Vac_perc_2022,
	MAX(CASE WHEN EXTRACT(year FROM date) = 2023 THEN running_vaccinated_percentage END) Vac_perc_2023
FROM (SELECT *, ROUND((running_total_vaccinated/population)*100,4) AS running_vaccinated_percentage
FROM popvactemp
)
GROUP BY location
ORDER BY vac_perc_2021 DESC

-- Note: often no differernce between 2022 and 2023 because no new vaccinations or missing data on new vaccinations.
-- Note: People can get vaccinated more than once, therefore the vaccination percentage can be larger than 100%.


-- Create view for Data visualisation to be created later
CREATE VIEW populationVac AS 
SELECT D.continent, D.location, D.date, D.population, vac.new_vaccinations, (SUM(vac.new_vaccinations) OVER(PARTITION BY D.location ORDER BY D.location, D.date))::bigint AS Running_total_vaccinated
FROM coviddeaths AS D
LEFT JOIN covidvaccinations AS vac
ON D.date = vac.date
	AND D.location = vac.location
WHERE D.continent IS NOT NULL



