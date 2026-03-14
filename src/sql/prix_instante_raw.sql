CREATE TABLE IF NOT EXISTS prix_instante_raw (
    id INT,
    latitude FLOAT,
    longitude FLOAT,
    cp STRING,
    adresse STRING,
    ville STRING,
    services STRING,
    gazole_prix FLOAT,
    gazole_maj TIMESTAMP,
    horaires STRING,
    sp95_maj TIMESTAMP,
    sp95_prix FLOAT,
    sp98_maj TIMESTAMP,
    sp98_prix FLOAT
)
