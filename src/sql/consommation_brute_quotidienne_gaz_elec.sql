CREATE OR REPLACE TABLE consommation_brute_quotidienne_gaz_elec_raw (
    "id" INT,
    "Date - Heure" TIMESTAMP,
    "Date" DATE,
    "Heure" STRING,
    "Consommation brute gaz (MW PCS 0°C) - GRTgaz" INT,
    "Statut - GRTgaz" STRING,
    "Consommation brute gaz (MW PCS 0°C) - Teréga" INT,
    "Statut - Teréga" STRING,
    "Consommation brute gaz totale (MW PCS 0°C)" INT,
    "Consommation brute électricité (MW)- RTE" INT,
    "Statut - RTE" STRING,
    "Consommation brute totale (MW)" INT,
    "flag_ignore" BOOLEAN
)
