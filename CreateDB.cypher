-- Load the films and their directors and producers
LOAD CSV WITH HEADERS FROM "file:///films.csv" 
    AS row 
    UNWIND split(row.producer, ",") AS producer
    MERGE (f:Film {name: trim(row.title), 
        opening: row.opening_crawl})
    MERGE (d:Person {name: trim(row.director)}) 
    MERGE (f)-[:DIRECTED_BY]->(d)
    MERGE (p:Person {name: trim(producer)})
    MERGE (p)<-[:PRODUCED_BY]-(f);



-- Load the characters
LOAD CSV WITH HEADERS FROM "file:///characters.csv" 
    AS row
    MERGE (c:Character {name: row.name})
    FOREACH(
        it IN 
            CASE row.homeworld WHEN "None" 
                THEN null 
                WHEN "Unknown" THEN null
                ELSE trim(row.homeworld) 
            END | 
            MERGE (p:Planet {name: it})     
            MERGE (c)-[:IS_HOMEWORLD]->(p) ) 
    FOREACH(
        it IN 
            CASE row.species WHEN "Unknown" 
                THEN null 
                ELSE trim(row.species) 
            END | 
            MERGE (s:Species {name: it})
            MERGE (c)-[:IS_OF_SPECIE]->(s)
            )
    SET 
        c.gender = CASE row.gender WHEN "None" 
                THEN null 
                ELSE row.gender 
            END,
        c.height = toFloatOrNull(row.height),
        c.weight = toFloatOrNull(row.weight),
        c.born = toIntegerOrNull(row.year_born),
        c.died = toIntegerOrNull(row.year_died),
        c.descripcion = row.description;


-- Load the planets
LOAD CSV WITH HEADERS FROM "file:///planets.csv" 
    AS row
    UNWIND split(row.terrain, ',') AS terrain
    UNWIND split(row.climate, ',') AS climate
    UNWIND split(row.residents, ",") AS residents
    UNWIND split(row.films, ",") AS film
    MERGE (p:Planet {name: trim(row.name)})
    MERGE (t:Terrain {name: trim(terrain)})
    MERGE (c:Climate {name: trim(climate)})
    MERGE (f:Film {name: trim(film)})
    MERGE (p)-[:HAS_TERRAIN]->(t)
    MERGE (p)-[:HAS_CLIMATE]->(c)
    MERGE (p)-[:APPEARS_IN]->(f)
    SET 
        p.diameter = toIntegerOrNull(row.diameter),
        p.rotation_period = toIntegerOrNull(row.rotation_period),
        p.orbital_period = toIntegerOrNull(row.orbital_period),
        p.gravity = toFloat(replace(row.gravity,"standard","")),
        p.population = toFloatOrNull(row.population),
        p.surface_water = toIntegerOrNull(row.surface_water);


-- Load the organizations

LOAD CSV WITH HEADERS FROM "file:///organizations.csv" 
    AS row
    UNWIND split(row.leader, ",") AS leader
    UNWIND split(row.members, ",") AS member
    UNWIND split(row.films, ",") AS film
    MERGE (o:Organization {name: row.name})
    MERGE (c:Character {name: trim(member)})
    MERGE (f:Film {name: trim(film)})
    MERGE (o)<-[:LEADER_OF]-(c)
    MERGE (o)-[:APPEARS_IN]->(f)
    FOREACH(
        it IN 
            CASE trim(row.afiliation) 
                WHEN "None" 
                THEN null 
                ELSE trim(row.afiliation) 
            END | 
            MERGE (a:Affiliation {name: it})
            MERGE (o)-[:BELONGS_TO]->(a)
            )
    SET 
        o.founded = toInteger(row.founded), 
        o.dissolved = toInteger(row.dissolved),
        o.description = row.description;

-- Load the species
LOAD CSV WITH HEADERS FROM "file:///species.csv" 
    AS row
    MERGE (s:Species {name: row.name})
    FOREACH(
        it IN 
            CASE trim(replace(row.classification, "Unknown",""))
                WHEN "" THEN null 
                ELSE trim(row.classification) 
            END | 
            MERGE (g:Genus {name: it})     
            MERGE (s)-[:BELONGS_TO]->(g) ) 
    FOREACH(
        it IN 
            CASE trim(replace(replace(row.homeworld, "Unknown",""), "Various", "" ))
                WHEN "" THEN null 
                ELSE trim(row.homeworld)
            END | 
            MERGE (p:Planet {name: it})     
            MERGE (s)-[:IS_HOMEWORLD]->(g) ) 
    SET 
        s.designation = row.designation,
        s.average_height = toFloatOrNull(row.average_height),
        s.average_lifespan = toIntegerOrNull(row.average_lifespan),
        s.language = row.language;

-- Load the Vehicles
LOAD CSV WITH HEADERS FROM "file:///vehicles.csv" 
    AS row
    UNWIND CASE row.pilots 
            WHEN "None" THEN NULL 
            ELSE split(row.pilots, ",") 
        END AS pilot
    UNWIND split(row.films, ",") AS film
    MERGE (v:Vehicle {name: row.name})
    MERGE (m:Manufacturer {name: trim(row.manufacturer) })
    MERGE (cl:Class {name: row.vehicle_class})
    MERGE (p:Character {name: trim(pilot)})
    MERGE (f:Film {name: trim(film)})
    MERGE (v)-[:MANUFACTURED_BY]->(m)
    MERGE (v)-[:VEHICLE_CLASS]->(cl)
    MERGE (p)-[:PILOTED]->(v)
    MERGE (v)-[:APPEARS_IN]->(f)

    SET 
        v.model = row.model,
        v.cost = toFloatOrNull(row.cost_in_credits),
        v.length = toFloatOrNull(row.length),
        v.max_speed = toFloatOrNull(row.max_atmosphering_speed),
        v.crew = toIntegerOrNull(row.crew),
        v.passengers = toIntegerOrNull(row.passengers),
        v.cargo_capacity = toFloatOrNull(row.cargo_capacity);


-- Load the Quotes and its relations with their author and film where they used it
LOAD CSV WITH HEADERS FROM "file:///quotes.csv" 
    AS row 
    MERGE (q:Quote {text: trim(row.quote)})
    MERGE (c:Character {name: trim(row.character_name)})
    MERGE (f:Film {name: trim(row.source)})
    MERGE (q)-[:QUOTE_FROM]->(c)
    MERGE (q)-[:SAID_IN]->(f);

-- Load the Starships and its related data
LOAD CSV WITH HEADERS FROM "file:///starships.csv" 
    AS row 
    UNWIND CASE row.pilots 
            WHEN "None" THEN null 
            ELSE split(row.pilots, ",") 
        END AS pilot
    UNWIND split(row.films, ",") AS film
    MERGE (s:Starship {name: row.name})
    MERGE (m:Manufacturer {name: trim(row.manufacturer) })
    MERGE (cl:Class {name: row.starship_class})
    MERGE (p:Character {name: trim(pilot)})
    MERGE (f:Film {name: trim(film)})
    MERGE (s)-[:MANUFACTURED_BY]->(m)
    MERGE (s)-[:STARSHIP_CLASS]->(cl)
    MERGE (p)-[:PILOTED]->(s)
    MERGE (s)-[:APPEARS_IN]->(f)
    SET 
        s.model = row.model,
        s.cost = toFloatOrNull(row.cost_in_credits),
        s.length = toFloatOrNull(row.length),
        s.max_speed = toFloatOrNull(row.max_atmosphering_speed),
        s.crew = toIntegerOrNull(row.crew),
        s.passengers = toIntegerOrNull(row.passengers),
        s.cargo_capacity = toFloatOrNull(row.cargo_capacity),
        s.hyperdrive_rating = toFloatOrNull(row.hyperdrive_rating);

-- Load the Weapons and their related data
LOAD CSV WITH HEADERS FROM "file:///weapons.csv" 
    AS row 
    UNWIND CASE row.manufacturer WHEN "Various" THEN NULL ELSE split(row.manufacturer, ",") END AS manufacturer
    UNWIND split(row.type, "/") AS weapon_type
    UNWIND split(row.films, ",") AS film
    MERGE (w:Weapon {name: row.name})
    MERGE (wt:WeaponType {name: trim(weapon_type)})
    MERGE (m:Manufacturer {name: trim(manufacturer) })
    MERGE (f:Film {name: trim(film)})
    MERGE (w)-[:MANUFACTURED_BY]->(m)
    MERGE (w)-[:WEAPON_TYPE]->(wt)
    MERGE (w)-[:APPEARS_IN]->(f)
    SET 
        w.model = row.model,
        w.cost = toFloatOrNull(row.cost_in_credits),
        w.length = toFloatOrNull(row.length),
        w.description = row.description