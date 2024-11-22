-- Example usage:
-- 1. run snowsql from this directory
-- 2. !source stage_to_snowflake.sql

-- Upload the environment configuration file
PUT 'file:///Users/aeldeeb/Downloads/cc-insights-streamlit/environment.yml'
    @MYDB.MY_SCHEMA.STAGE_CC_INSIGHTS_STREAMLIT
    OVERWRITE = TRUE
    AUTO_COMPRESS = FALSE;

-- Upload main Streamlit application file
PUT 'file:///Users/aeldeeb/Downloads/cc-insights-streamlit/Contact_Center_Insights.py'
    @MYDB.MY_SCHEMA.STAGE_CC_INSIGHTS_STREAMLIT
    OVERWRITE = TRUE
    AUTO_COMPRESS = FALSE;

-- Upload additional pages to the pages subdirectory
PUT 'file:///Users/aeldeeb/Downloads/cc-insights-streamlit/pages/*.py'
    @MYDB.MY_SCHEMA.STAGE_CC_INSIGHTS_STREAMLIT/pages/
    OVERWRITE = TRUE
    AUTO_COMPRESS = FALSE;

-- Upload call-center-cubicles-pexels-mid-blue.jpg
PUT 'file:///Users/aeldeeb/Downloads/cc-insights-streamlit/call-center-cubicles-pexels-mid-blue.jpg'
    @MYDB.MY_SCHEMA.STAGE_CC_INSIGHTS_STREAMLIT
    OVERWRITE = TRUE
    AUTO_COMPRESS = FALSE;

LIST @MYDB.MY_SCHEMA.STAGE_CC_INSIGHTS_STREAMLIT;