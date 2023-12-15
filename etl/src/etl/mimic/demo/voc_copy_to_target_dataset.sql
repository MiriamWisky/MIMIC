
CREATE OR REPLACE TABLE voc_concept AS
SELECT * FROM concept
;

CREATE OR REPLACE TABLE voc_concept_relationship AS
SELECT * FROM concept_relationship
;

CREATE OR REPLACE TABLE voc_vocabulary AS
SELECT * FROM vocabulary
;

-- not affected by custom mapping

CREATE OR REPLACE TABLE voc_domain AS
SELECT * FROM domain
;
CREATE OR REPLACE TABLE voc_concept_class AS
SELECT * FROM concept_class
;
CREATE OR REPLACE TABLE voc_relationship AS
SELECT * FROM relationship
;
CREATE OR REPLACE TABLE voc_concept_synonym AS
SELECT * FROM concept_synonym
;
CREATE OR REPLACE TABLE voc_concept_ancestor AS
SELECT * FROM concept_ancestor
;
CREATE OR REPLACE TABLE voc_drug_strength AS
SELECT * FROM drug_strength
;
