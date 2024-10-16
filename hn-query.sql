DECLARE term_list ARRAY <STRING>;
SET term_list = ['virus','covid','vaccine','rna','dna','evolution','spread','pandemic','quarantine','lockdown','mask','remote','zoom','telemedicine','bitcoin','freedom','startup','corporation','equity','stock','index','etf','rsu','tc','compensation','salary','innovation','blockchain','ai','ml','gpu','python','rust','java','cpp','javascript','hardware','cloud','b2b','saas','tensorflow','pytorch','cuda','nvidia','faang','meta','amazon','netflix','microsoft','google','facebook','instagram','snapchat','airbnb','stripe','coinbase','binance','ftx','ethereum','nft','defi','crypto','web3','ipo','venture','angel','iot','vr','ar','quantum','golang','kotlin','swift','typescript','react','vue','angular','opencv','scikit','huggingface','openai','gpt','marketplace','freemium','subscription','growth','lean','apple','uber','lyft','palantir','snowflake','tiktok','twitter','linkedin','discord','slack','amd','intel','arm','qualcomm','tsmc','musk','altman','graham','andreessen','thiel','nadella','pichai','zuckerberg','bezos','5g','edge','autonomous','robotics','drones','space','biotech','git','docker','kubernetes','devops','cicd','agile','scrum','bigdata','analytics','tableau','spark','hadoop','cybersecurity','encryption','zerotrust','gdpr','privacy','layoff','layoffs','tesla','spacex'];

WITH comments_src AS (
  -- Date-Level
  -- Unique message_id generated 
  -- Non-empty comments
    SELECT DATE(timestamp) AS report_date 
    , FARM_FINGERPRINT(CONCAT(COALESCE(url,'-'), COALESCE(SAFE_CAST(id AS STRING),'-'))) AS message_id
    , text
  FROM `bigquery-public-data.hacker_news.full`
  WHERE TRUE 
    AND DATE(timestamp) BETWEEN '2017-10-01' AND '2024-10-01'
    AND type = 'comment'
    AND text is not null 
), tokens AS (
  -- Tokenising text into array
  SELECT report_date
    , message_id
    , text_analyze(text) text_list
  FROM comments_src
), terms AS (
  -- Unnesting the array into plain long table
  SELECT report_date 
    , message_id
    , term
  FROM tokens, unnest(text_list) as term
), overall_count AS (
  -- Total daily messages, not a window for clarity
  SELECT report_date
    , COUNT(DISTINCT message_id) AS all_messages
  FROM terms
  GROUP BY ALL 
), overall_count_term AS (
  -- Overall message stats multiplied by each term from initiated list of terms
  SELECT report_date
    , term
    , all_messages
  FROM overall_count
  , UNNEST(term_list) AS term 
), term_count AS (
  -- Counting every term in dataset
  SELECT report_date
     , term 
     , COUNT(DISTINCT message_id) AS term_messages
   FROM terms
   GROUP BY ALL 
) -- Join of 2 aggregated sources - every term from the list covered 
  SELECT o.report_date
     , o.term
     , COALESCE(o.all_messages, 0) AS all_messages
     , COALESCE(t.term_messages,0) AS term_messages
   FROM overall_count_term o 
   LEFT JOIN term_count t USING(report_date, term)
