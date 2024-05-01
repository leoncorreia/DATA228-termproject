CREATE KEYSPACE IF NOT EXISTS group4 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE group4;

CREATE TABLE IF NOT EXISTS prescription_data (
    uuid uuid,
    Geographic_Level text,
    Geographic_Code text,
    Geographic_Description text,
    Brand_Name text,
    Generic_Name text,
    Total_Prescribers int,
    Total_Claims int,
    Tot_30day_Fills float,
    Total_Drug_Cost float,
    Total_Beneficiaries int,
    Opioid_Drug_Flag text,
    Opioid_Long_Acting_Drug_Flag text,
    Antibiotic_Drug_Flag text,
    Antpsyct_Drug_Flag text,
    PRIMARY KEY (uuid)
);
