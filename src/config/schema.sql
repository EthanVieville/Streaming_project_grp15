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

-- STAT 1 :
CREATE TABLE IF NOT EXISTS satisfaction_by_marriage_type (
  Marriage_Type TEXT,
  Marital_Satisfaction TEXT,
  Count BIGINT
);
