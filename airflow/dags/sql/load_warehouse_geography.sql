
with lookup as (
SELECT 'Moskva (Moscow)' as cityname, 'Russia' as countryname, 'Eastern Europe' as regionname UNION ALL
SELECT 'London', 'UK', 'Western Europe' UNION ALL
SELECT 'St Petersburg', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Berlin', 'Germany', 'Central Europe' UNION ALL
SELECT 'Madrid', 'Spain', 'Southern Europe' UNION ALL
SELECT 'Roma', 'Italy', 'Southern Europe' UNION ALL
SELECT 'Kiev', 'Ukraine', 'Eastern Europe' UNION ALL
SELECT 'Paris', 'France', 'Western Europe' UNION ALL
SELECT 'Bucuresti (Bucharest)', 'Romania', 'Eastern Europe' UNION ALL
SELECT 'Budapest', 'Hungary', 'Central Europe' UNION ALL
SELECT 'Hamburg', 'Germany', 'Central Europe' UNION ALL
SELECT 'Minsk', 'Belarus', 'Eastern Europe' UNION ALL
SELECT 'Warszawa (Warsaw)', 'Poland', 'Central Europe' UNION ALL
SELECT 'Beograd (Belgrade)', 'Serbia', 'Eastern Europe' UNION ALL
SELECT 'Wien (Vienna)', 'Austria', 'Central Europe' UNION ALL
SELECT 'Kharkov', 'Ukraine', 'Eastern Europe' UNION ALL
SELECT 'Barcelona', 'Spain', 'Southern Europe' UNION ALL
SELECT 'Novosibirsk', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Nizhny Novgorod', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Milano (Milan)', 'Italy', 'Southern Europe' UNION ALL
SELECT 'Ekaterinoburg', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'München (Munich)', 'Germany', 'Central Europe' UNION ALL
SELECT 'Praha (Prague)', 'Czech Republic', 'Central Europe' UNION ALL
SELECT 'Samara', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Omsk', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Sofia', 'Bulgaria', 'Eastern Europe' UNION ALL
SELECT 'Dnepropetrovsk', 'Ukraine', 'Eastern Europe' UNION ALL
SELECT 'Kazan', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Ufa', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Chelyabinsk', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Donetsk ', 'Ukraine', 'Eastern Europe' UNION ALL
SELECT 'Napoli (Naples)', 'Italy', 'Southern Europe' UNION ALL
SELECT 'Birmingham', 'UK', 'Western Europe' UNION ALL
SELECT 'Perm', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Rostov-Na-Donu', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Odessa', 'Ukraine', 'Eastern Europe' UNION ALL
SELECT 'Volgograd', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Köln (Cologne)', 'Germany', 'Central Europe' UNION ALL
SELECT 'Torino (Turin)', 'Italy', 'Southern Europe' UNION ALL
SELECT 'Voronezh', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Krasnoyarsk', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Saratov', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Zagreb', 'Croatia', 'Eastern Europe' UNION ALL
SELECT 'Zaporozhye', 'Ukraine', 'Eastern Europe' UNION ALL
SELECT 'Lódz', 'Poland', 'Central Europe' UNION ALL
SELECT 'Marseille', 'France', 'Western Europe' UNION ALL
SELECT 'Riga', 'Latvia', 'Northern Europe' UNION ALL
SELECT 'Lvov', 'Ukraine', 'Eastern Europe' UNION ALL
SELECT 'Athinai (Athens)', 'Greece', 'Southern Europe' UNION ALL
SELECT 'Salonika', 'Greece', 'Southern Europe' UNION ALL
SELECT 'Stockholm', 'Sweden', 'Northern Europe' UNION ALL
SELECT 'Kraków', 'Poland', 'Central Europe' UNION ALL
SELECT 'Valencia', 'Spain', 'Southern Europe' UNION ALL
SELECT 'Amsterdam', 'Netherlands', 'Western Europe' UNION ALL
SELECT 'Leeds', 'UK', 'Western Europe' UNION ALL
SELECT 'Tolyatti', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Kryvy Rig', 'Ukraine', 'Eastern Europe' UNION ALL
SELECT 'Sevilla', 'Spain', 'Southern Europe' UNION ALL
SELECT 'Palermo', 'Italy', 'Southern Europe' UNION ALL
SELECT 'Ulyanovsk', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Kishinev', 'Moldova', 'Eastern Europe' UNION ALL
SELECT 'Genova', 'Italy', 'Southern Europe' UNION ALL
SELECT 'Izhevsk', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Frankfurt Am Main', 'Germany', 'Central Europe' UNION ALL
SELECT 'Krasnodar', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Wroclaw (Breslau)', 'Poland', 'Central Europe' UNION ALL
SELECT 'Glasgow', 'UK', 'Western Europe' UNION ALL
SELECT 'Yaroslave', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Khabarovsk', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Vladivostok', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Zaragoza', 'Spain', 'Southern Europe' UNION ALL
SELECT 'Essen', 'Germany', 'Central Europe' UNION ALL
SELECT 'Rotterdam', 'Netherlands', 'Western Europe' UNION ALL
SELECT 'Irkutsk', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Dortmund', 'Germany', 'Central Europe' UNION ALL
SELECT 'Stuttgart', 'Germany', 'Central Europe' UNION ALL
SELECT 'Barnaul', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Vilnius', 'Lithuania', 'Northern Europe' UNION ALL
SELECT 'Poznan', 'Poland', 'Central Europe' UNION ALL
SELECT 'Düsseldorf', 'Germany', 'Central Europe' UNION ALL
SELECT 'Novokuznetsk', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Lisboa (Lisbon)', 'Portugal', 'Southern Europe' UNION ALL
SELECT 'Helsinki', 'Finland', 'Northern Europe' UNION ALL
SELECT 'Málaga', 'Spain', 'Southern Europe' UNION ALL
SELECT 'Bremen', 'Germany', 'Central Europe' UNION ALL
SELECT 'Sheffield', 'UK', 'Western Europe' UNION ALL
SELECT 'Sarajevo', 'Bosnia', 'Eastern Europe' UNION ALL
SELECT 'Penza', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Ryazan', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Orenburg', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Naberezhnye Tchelny', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Duisburg', 'Germany', 'Central Europe' UNION ALL
SELECT 'Lipetsk', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Hannover', 'Germany', 'Central Europe' UNION ALL
SELECT 'Mykolaiv ', 'Ukraine', 'Eastern Europe' UNION ALL
SELECT 'Tula', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Oslo', 'Norway', 'Northern Europe' UNION ALL
SELECT 'Tyumen', 'Russia', 'Eastern Europe' UNION ALL
SELECT 'Kobenhavn (Copenhagen)', 'Denmark', 'Northern Europe' UNION ALL
SELECT 'Kemerovo', 'Russia', 'Eastern Europe'

) 

INSERT INTO warehouse.dim_geography(cityname, countryname, regionname)

SELECT distinct city, countryname, regionname 
from import.products e
join lookup l on e.city = l.cityname

where city not in (select cityname from warehouse.dim_geography)
