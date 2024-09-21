CREATE CONSTRAINT ON (p:Species) ASSERT p.id IS UNIQUE;
CREATE CONSTRAINT ON (p:Character) ASSERT p.id IS UNIQUE;
CREATE CONSTRAINT ON (p:Film) ASSERT p.id IS UNIQUE;
CREATE CONSTRAINT ON (p:Planet) ASSERT p.id IS UNIQUE;
CREATE CONSTRAINT ON (p:Organization) ASSERT p.id IS UNIQUE;
CREATE CONSTRAINT ON (p:Affiliation) ASSERT p.id IS UNIQUE;

LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/planetacomputer/neo4j-starwars/main/dataset/films.csv" 
    AS row 
    UNWIND split(row.producer, ",") AS producer
    MERGE (f:Film {name: trim(row.title), 
        opening: row.opening_crawl})
    MERGE (d:Person {name: trim(row.director)}) 
    MERGE (f)-[:DIRECTED_BY]->(d)
    MERGE (p:Person {name: trim(producer)})
    MERGE (p)<-[:PRODUCED_BY]-(f);

LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/planetacomputer/neo4j-starwars/main/dataset/characters.csv" 
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

LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/planetacomputer/neo4j-starwars/main/dataset/planets.csv" 
    AS row
    UNWIND split(row.residents, ",") AS residents
    UNWIND split(row.films, ",") AS film
    MERGE (p:Planet {name: trim(row.name)})
    MERGE (f:Film {name: trim(film)})
    MERGE (p)-[:APPEARS_IN]->(f)
    SET 
        p.diameter = toIntegerOrNull(row.diameter),
        p.rotation_period = toIntegerOrNull(row.rotation_period),
        p.orbital_period = toIntegerOrNull(row.orbital_period),
        p.gravity = toFloatOrNull(replace(row.gravity,"standard","")),
        p.population = toFloatOrNull(row.population),
        p.surface_water = toIntegerOrNull(row.surface_water);

LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/planetacomputer/neo4j-starwars/main/dataset/organizations.csv" 
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
            CASE trim(row.affiliation) 
                WHEN "None" 
                THEN null 
                ELSE trim(row.affiliation) 
            END | 
            MERGE (a:Affiliation {name: it})
            MERGE (o)-[:BELONGS_TO]->(a)
            )
    SET 
        o.founded = toIntegerOrNull(row.founded), 
        o.dissolved = toIntegerOrNull(row.dissolved),
        o.description = row.description;

LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/planetacomputer/neo4j-starwars/main/dataset/species.csv" 
    AS row
    MERGE (s:Species {name: row.name})
    FOREACH(
        it IN 
            CASE trim(replace(row.classification, "Unknown",""))
                WHEN "" THEN null 
                ELSE trim(row.classification) 
            END |  
            MERGE (g:Classification {name: it})  
            MERGE (s)-[:BELONGS_TO]->(g) )  
    FOREACH(
        it IN 
            CASE trim(replace(replace(row.homeworld, "Unknown",""), "Various", "" ))
                WHEN "" THEN null 
                ELSE trim(row.homeworld)
            END | 
            MERGE (p:Planet {name: it})     
            MERGE (s)-[:IS_HOMEWORLD]->(p) ) 
    SET 
        s.designation = row.designation,
        s.average_height = toFloatOrNull(row.average_height),
        s.average_lifespan = toIntegerOrNull(row.average_lifespan),
        s.language = row.language;
