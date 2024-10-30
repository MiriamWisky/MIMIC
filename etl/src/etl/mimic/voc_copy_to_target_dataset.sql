
CREATE TABLE voc_concept AS
SELECT * FROM concept
;

CREATE TABLE voc_concept_relationship AS
SELECT * FROM concept_relationship
;

CREATE TABLE voc_vocabulary AS
SELECT * FROM vocabulary
;

-- not affected by custom mapping

CREATE TABLE voc_domain AS
SELECT * FROM domain
;
CREATE TABLE voc_concept_class AS
SELECT * FROM concept_class
;
CREATE TABLE voc_relationship AS
SELECT * FROM relationship
;
CREATE TABLE voc_concept_synonym AS
SELECT * FROM concept_synonym
;
CREATE TABLE voc_concept_ancestor AS
SELECT * FROM concept_ancestor
;
CREATE TABLE voc_drug_strength AS
SELECT * FROM drug_strength
;
