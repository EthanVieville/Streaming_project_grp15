-- FULL DATA
CREATE TABLE IF NOT EXISTS marriages (
  ID TEXT PRIMARY KEY,
  Marriage_Type TEXT,
  Age_at_Marriage TEXT,
  Gender TEXT,
  Education_Level TEXT,
  Caste_Match TEXT,
  Religion TEXT,
  Parental_Approval TEXT,
  Urban_Rural TEXT,
  Dowry_Exchanged TEXT,
  Marital_Satisfaction TEXT,
  Divorce_Status TEXT,
  Children_Count TEXT,
  Income_Level TEXT,
  Years_Since_Marriage TEXT,
  Spouse_Working TEXT,
  "Inter-Caste" TEXT,
  "Inter-Religion" TEXT
);

-- Satisfaction par mariage, pour gens non divorcé
CREATE TABLE IF NOT EXISTS satisfaction_by_marriage_type (
  Marriage_Type TEXT,
  Marital_Satisfaction TEXT,
  Count BIGINT
);


-- Satisfaction par mariage, par tranche d'age, pas divorcé
CREATE TABLE IF NOT EXISTS satisfaction_by_age_range (
  Age_Range TEXT,
  Marital_Satisfaction TEXT,
  Count BIGINT
);

-- Satisfaction par education level
CREATE TABLE IF NOT EXISTS satisfaction_by_education_level (
  Education_Level TEXT,
  Marital_Satisfaction TEXT,
  Count BIGINT
);

-- Satisfaction par relgion WOUAHHH??????
CREATE TABLE IF NOT EXISTS satisfaction_by_religion_arranged (
  Religion TEXT,
  Marital_Satisfaction TEXT,
  Count BIGINT
);

-- Satisfaction par inter religion
CREATE TABLE IF NOT EXISTS satisfaction_by_inter_religion (
  Religion TEXT,
  Marital_Satisfaction TEXT,
  Count BIGINT
);

-- Satisfaction par revenus
CREATE TABLE IF NOT EXISTS satisfaction_by_income_level (
  Income_Level TEXT,
  Marital_Satisfaction TEXT,
  Count BIGINT
);

-- Satisfaction par approbation des parents
CREATE TABLE IF NOT EXISTS satisfaction_by_parental_approval (
  Parental_Approval TEXT,
  Marital_Satisfaction TEXT,
  Count BIGINT
);

-- DIVORCE PAR RELIGION AHAHAHAHAHAHAH
CREATE TABLE IF NOT EXISTS divorces_by_religion (
  Religion TEXT,
  Count BIGINT
);

-- DIVORCE PAR EDUCATION
CREATE TABLE IF NOT EXISTS divorces_by_education (
  Education_Level TEXT,
  Count BIGINT
);

-- DIVORCE PAR SPOUSE WORKING
CREATE TABLE IF NOT EXISTS divorces_by_spouse_working (
  Spouse_Working TEXT,
  Count BIGINT
);

-- FIN : combien d'enfants ??
CREATE TABLE IF NOT EXISTS children_count_by_profile (
  Children_Count TEXT,
  Count BIGINT
);



