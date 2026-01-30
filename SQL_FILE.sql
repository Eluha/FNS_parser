CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.raw_dump_data;

CREATE TABLE raw.raw_dump_data (okved VARCHAR(10) NOT NULL,
year SMALLINT,
page SMALLINT,
region_name VARCHAR(200),
json_data JSONB NOT NULL);

CREATE TABLE IF NOT EXISTS raw.raw_dump_data_search_params (okved VARCHAR(10) NOT NULL,
year SMALLINT,
region_name VARCHAR(200),
total_page smallint NOT NULL,
total_elements integer NOT NULL,
insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

DROP TABLE IF EXISTS raw.header_org_info CASCADE;
CREATE TABLE IF NOT EXISTS raw.header_org_info
(id INTEGER NOT NULL PRIMARY KEY,
 inn VARCHAR(20),
 shortName TEXT,
 ogrn VARCHAR(20),
 index VARCHAR(20),
 region VARCHAR(200),
 district VARCHAR(200),
 city VARCHAR(200),
 okved2 VARCHAR(200),
 okopf INTEGER,
 okato INTEGER,
 okpo INTEGER,
 okfs INTEGER,
 statusCode VARCHAR(200),
 statusDate date);

CREATE TABLE IF NOT EXISTS raw.raw_organization_dump_data 
(id INTEGER PRIMARY KEY,
 last_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 payload JSONB NOT NULL);

CREATE TABLE IF NOT EXISTS raw.raw_organization_bfo_dump_data 
(id INTEGER PRIMARY KEY,
 last_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 payload JSONB NOT NULL);


DROP TABLE IF EXISTS "raw"."org_info" CASCADE;
CREATE TABLE "raw"."org_info" (
 "id" INTEGER NOT NULL PRIMARY KEY REFERENCES raw_organization_dump_data("id") ON DELETE CASCADE,
 "inn" VARCHAR(20),
 "shortName" TEXT,
 "ogrn" VARCHAR(20),
 "index" VARCHAR(20),
 "region" VARCHAR(200),
 "district" VARCHAR(200),
 "city" VARCHAR(200),
 "okved2_id" VARCHAR(200) REFERENCES dict_okved2(id) ON DELETE CASCADE,
 "okopf_id" INTEGER REFERENCES dict_okopf(id) ON DELETE CASCADE,
 "okato_id" INTEGER REFERENCES dict_okato(id) ON DELETE CASCADE,
 "okpo_id" INTEGER REFERENCES dict_okpo(id) ON DELETE CASCADE,
 "okfs_id" INTEGER REFERENCES dict_okfs(id) ON DELETE CASCADE,
 "statusCode" VARCHAR(200),
 "statusDate" date,
 "msp_id" INTEGER,
 "kpp" VARCHAR(100),
 "fullName" VARCHAR(1000),
 "registrationDate" DATE,
 "location.id" INTEGER,
 "location.name" VARCHAR(1000),
 "location.code" INTEGER,
 "location.latitude" FLOAT,
 "location.longitude" FLOAT,
 "location.type" VARCHAR(100),
 "location.parentId" INTEGER,
 "authorizedCapital" BIGINT,
 "active" BOOLEAN);


DROP TABLE IF EXISTS "raw"."org_info.bfo" CASCADE;
CREATE TABLE "raw"."org_info.bfo" (
 "id" INTEGER NOT NULL REFERENCES "raw"."org_info"("id") ON DELETE CASCADE,
 "period" VARCHAR(10) NOT NULL,
 "publication" INTEGER,
 "actualBfoDate" DATE,
 "gainSum" INTEGER,
 "knd" VARCHAR(200),
 "hasAz" BOOLEAN,
 "hasKs" BOOLEAN,
 "actualCorrectionNumber" INTEGER,
 "actualCorrectionDate" DATE,
 "publishedCorrectionNumber" INTEGER,
 "publishedCorrectionDate" DATE,
 "actives" FLOAT,
 "isCb" VARCHAR(200),
 "mspCategory" INTEGER,
 "published" BOOLEAN,
 PRIMARY KEY ("id", "period"));

-- Создание таблицы с первичкой БФО
CREATE TABLE IF NOT EXISTS raw.raw_bfo_organization_dump_data 
(id INTEGER PRIMARY KEY,
 last_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 payload JSONB NOT NULL);



-----------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------- TRIGGERS ---------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------

-- Функция для расфасовки данных из шапки профиля организации
CREATE OR REPLACE FUNCTION raw.split_info_from_json() 
RETURNS TRIGGER AS $split_list_details$
BEGIN
    INSERT INTO raw.header_org_info(
        "id", "inn", "shortName", "ogrn", "index", "region", 
        "district", "city", "okved2", "okopf", "okato",
        "okpo", "okfs", "statusCode", "statusDate",
        "bfo.knd",
        "bfo.isCb",
        "bfo.hasAz",
        "bfo.hasKs",
        "bfo.period",
        "bfo.gainSum",
        "bfo.actualBfoDate",
        "bfo.actualCorrectionDate",
        "bfo.actualCorrectionNumber"
    )
    SELECT 
        (elements->>'id')::integer,
        (elements->>'inn')::varchar,
        (elements->>'shortName')::text,
        (elements->>'ogrn')::varchar,
        (elements->>'index')::varchar,
        (elements->>'region')::varchar,
        (elements->>'district')::varchar,
        (elements->>'city')::varchar,
        (elements->>'okved2')::varchar,
        (elements->>'okopf')::integer,
        (elements->>'okato')::integer,
        (elements->>'okpo')::integer,
        (elements->>'okfs')::integer,
        (elements->>'statusCode')::varchar, -- Вы поменяли на varchar, убедитесь, что в таблице тоже varchar
        (elements->>'statusDate')::date,
        (elements->'bfo'->>'knd')::varchar,
        (elements->'bfo'->>'isCb')::boolean,
        (elements->'bfo'->>'hasAz')::boolean,
        (elements->'bfo'->>'hasKs')::boolean,
        (elements->'bfo'->>'period')::varchar,
        (elements->'bfo'->>'gainSum')::integer,
        (elements->'bfo'->>'actualBfoDate')::date,
        (elements->'bfo'->>'actualCorrectionDate')::date,
        (elements->'bfo'->>'actualCorrectionNumber')::integer
    FROM jsonb_array_elements(NEW.payload->'content') AS elements
    ON CONFLICT ("id") DO NOTHING;
    
    RETURN NEW;
END;
$split_list_details$ LANGUAGE plpgsql;

CREATE TRIGGER split_list_details
AFTER INSERT ON raw.raw_dump_data
FOR EACH ROW
EXECUTE FUNCTION split_info_from_json();




DROP FUNCTION IF EXISTS raw.split_org_info() CASCADE;
-- Функция распаковки JSONB

CREATE OR REPLACE FUNCTION raw.split_org_info() RETURNS TRIGGER AS $insert_bfo_and_orgs$
    DECLARE
        elements JSONB;
    BEGIN
        elements :=NEW.payload;
        
        IF (elements->'okato'->>'id') IS NOT NULL THEN
            INSERT INTO "raw"."dict_okato" ("id", "name")
            VALUES (
                (elements->'okato'->>'id')::integer,
                (elements->'okato'->>'name')::varchar
            ) ON CONFLICT ("id") DO NOTHING;
        END IF;

        -- OKFS
        IF (elements->'okfs'->>'id') IS NOT NULL THEN
            INSERT INTO "raw"."dict_okfs" ("id", "name")
            VALUES (
                (elements->'okfs'->>'id')::integer,
                (elements->'okfs'->>'name')::varchar
            ) ON CONFLICT ("id") DO NOTHING;
        END IF;

        -- OKOPF
        IF (elements->'okopf'->>'id') IS NOT NULL THEN
            INSERT INTO "raw"."dict_okopf" ("id", "name")
            VALUES (
                (elements->'okopf'->>'id')::integer,
                (elements->'okopf'->>'name')::varchar
            ) ON CONFLICT ("id") DO NOTHING;
        END IF;

        -- OKPO
        -- ИСПРАВЛЕНО: Источник теперь elements->'okpo', а не 'okopf'
        IF (elements->'okpo'->>'id') IS NOT NULL THEN
            INSERT INTO "raw"."dict_okpo" ("id", "name")
            VALUES (
                (elements->'okpo'->>'id')::integer,
                (elements->'okpo'->>'name')::varchar
            ) ON CONFLICT ("id") DO NOTHING;
        END IF;

        -- OKVED2
        -- ИСПРАВЛЕНО: Источник теперь elements->'okved2', а не 'okopf'
        IF (elements->'okved2'->>'id') IS NOT NULL THEN
            INSERT INTO "raw"."dict_okved2" ("id", "name")
            VALUES (
                (elements->'okved2'->>'id')::varchar,
                (elements->'okved2'->>'name')::varchar
            ) ON CONFLICT ("id") DO NOTHING;
        END IF;

        INSERT INTO "raw"."org_info" ("id", "inn", "shortName", "ogrn", "index",
               "region", "district", "city", "okved2_id", "okopf_id",
               "okato_id", "okpo_id", "okfs_id", "statusCode",
               "statusDate", "msp_id", "kpp", "fullName", "registrationDate",
               "location.id", "location.name", "location.code",
               "location.latitude", "location.longitude", "location.type",
               "location.parentId", "authorizedCapital", "active")
        VALUES(
(elements->>'id')::integer,
(elements->>'inn')::varchar, 
(elements->>'shortName')::text,
(elements->>'ogrn')::varchar,
(elements->>'index')::varchar,
(elements->>'region')::varchar,
(elements->>'district')::varchar,
(elements->>'city')::varchar,
(elements->'okved2'->>'id')::varchar,
(elements->'okopf'->>'id')::integer,
(elements->'okato'->>'id')::integer,
(elements->'okpo'->>'id')::integer,
(elements->'okfs'->>'id')::integer,
(elements->>'statusCode')::varchar,
(elements->>'statusDate')::date,
(elements->>'msp_id')::integer ,
(elements->>'kpp')::varchar,
(elements->>'fullName')::varchar,
(elements->>'registrationDate')::date,
(elements->'location'->>'id')::integer ,
(elements->'location'->>'name')::varchar,
(elements->'location'->>'code')::integer,
(elements->'location'->>'latitude')::float,
(elements->'location'->>'longitude')::float,
(elements->'location'->>'type')::varchar,
(elements->'location'->>'parentId')::integer ,
CAST(elements->>'authorizedCapital' AS NUMERIC)::BIGINT ,
(elements->>'active')::boolean)
ON CONFLICT ("id") DO NOTHING;
        
        INSERT INTO "raw"."org_info.bfo" ("id", "period", "publication", "actualBfoDate", "gainSum",
               "knd", "hasAz", "hasKs", "actualCorrectionNumber", "actualCorrectionDate",
               "publishedCorrectionNumber", "publishedCorrectionDate", "actives", "isCb",
               "mspCategory", "published")

        SELECT (elements->>'id')::integer,
(bfo_data->>'period')::varchar,
(bfo_data->>'publication')::integer,
(bfo_data->>'actualBfoDate')::date,
(bfo_data->>'gainSum')::integer,
(bfo_data->>'knd')::varchar,
(bfo_data->>'hasAz')::boolean,
(bfo_data->>'hasKs')::boolean,
(bfo_data->>'actualCorrectionNumber')::integer,
(bfo_data->>'actualCorrectionDate')::date,
(bfo_data->>'publishedCorrectionNumber')::integer,
(bfo_data->>'publishedCorrectionDate')::date,
(bfo_data->>'actives')::float,
(bfo_data->>'isCb')::varchar,
(bfo_data->>'mspCategory')::integer ,
(bfo_data->>'published')::boolean
        FROM jsonb_array_elements(elements ->'bfo') as bfo_data;
        RETURN NEW;
    END;
$insert_bfo_and_orgs$ LANGUAGE plpgsql;

CREATE TRIGGER insert_bfo_and_orgs
AFTER INSERT ON raw.raw_organization_dump_data
FOR EACH ROW
EXECUTE FUNCTION split_org_info();

-----------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------- TRIGGERS ---------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS dict_okved2;
DROP TABLE IF EXISTS dict_okopf;
DROP TABLE IF EXISTS dict_okato;
DROP TABLE IF EXISTS dict_okpo;
DROP TABLE IF EXISTS dict_okfs;

CREATE TABLE IF NOT EXISTS raw.dict_okved2 ("id" INTEGER PRIMARY KEY,
                                            "name" VARCHAR(1000));
CREATE TABLE IF NOT EXISTS raw.dict_okopf ("id" INTEGER PRIMARY KEY,
                                           "name" VARCHAR(1000));
CREATE TABLE IF NOT EXISTS raw.dict_okato ("id" INTEGER PRIMARY KEY,
                                           "name" VARCHAR(1000));
CREATE TABLE IF NOT EXISTS raw.dict_okpo ("id" INTEGER PRIMARY KEY,
                                          "name" VARCHAR(1000));
CREATE TABLE IF NOT EXISTS raw.dict_okfs ("id" INTEGER PRIMARY KEY,
                                          "name" VARCHAR(1000));