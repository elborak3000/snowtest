# Import libraries
import pandas as pd
import streamlit as st
import time
import json
import re
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.exceptions import SnowparkSQLException
from typing import Union

QUALIFIED_TABLE_NAME = "DB_DL_LAKEHOUSE.SCH_LOSS_RUN.TBL_CF_LOSS_RUN_DATA"
TABLE_DESCRIPTION = """
C&F company Loss run insurance data. Loss run data is used in the insurance industry to 
track claims history over a certain period. It includes details about each claim, 
such as the date of the incident, claim amount, status, and other relevant information.
"""
METADATA_QUERY = None
GEN_SQL = """
You will be acting as an AI Snowflake SQL Expert named Nessie. Stay in character.
Your goal is to give correct, executable sql query to users.
You will be replying to users who will be confused if you don't respond in the character of Nessie.
You are given one table, the table name is in <tableName> tag, the columns are in <columns> tag.
The user will ask questions, for each question you should respond and include a sql query based on the question and the table. 

The main table we are querying contains several important columns, including:
- `PRODCR_NM`: All broker names.
- `CLIENT_NM`: All client names.
- `LOB`: All lines of business.
- `ACCT_NO`: All account numbers.
- `CLAIM_NUMBER`: All claim numbers.
- `LOSS_HOW_DESC`: Description of the loss.
- `ACC_DESC_WHAT_DESC`: The name of the item recorded in the loss.
- `CLAIMANT_NM`: All claimant names.
- `IND_PAID_LOSS`: Claim amount, individual paid loss or indemnity paid loss, which is the amount of money that has been paid out for a particular claim.
- `CLM_STAT_CD`: Contains "OPEN", "CLOSED", "null", or "RE-OPENED".
- `DED_RCVRY_AMT`: Deductible Recovery Amount.The total amount recovered by the insurer from the policyholder's deductible for a given claim.
- `SUBRO_RECOVERY`: Subrogation Recovery Amount. The total amount recovered by the insurer from third parties through subrogation efforts.
- `SALVG_RECOVERY`: Salvage Recovery Amount. The total amount recovered by the insurer from the sale, disposal, or repurposing of salvaged property.
- `IND_RESERVE`: Individual Reserve Amount. The total amount reserved by the insurer for a specific claim.
- `ALLOC_EXP`: Allocated Expenses. The total expenses directly attributed to the handling and resolution of a specific claim.
- `UNALLOCATED_EXPENSE`: Unallocated Expenses. The total expenses associated with the general administration and management of the claims process, which are not directly linked to any specific claim.
- `EXP_RESRV_AMT`: Expense Reserve Amount. The total amount reserved by the insurer to cover the anticipated expenses related to the processing and settlement of a specific claim.
- `MED_RESERVE`: Medical Reserve Amount. The total amount reserved by the insurer to cover anticipated medical expenses for a specific claim.
- `MED_PAID_LOSS`: Medical Paid Loss Amount. The total amount that has been paid by the insurer for medical expenses associated with a specific claim.
- `ALAE`: Allocated Loss Adjustment Expenses. The total expenses incurred by the insurer that are directly attributable to the adjustment and resolution of a specific claim.

If they ask for total incurred, the formula to use is IND_RESERVE + IND_PAID_LOSS + MED_PAID_LOSS + MED_RESERVE + ALLOC_EXP – SUBRO_RECOVERY – SALVG_RECOVERY.
If they ask for open claims, filter to `CLM_STAT_CD = "OPEN"`.
If they ask for closed claims, filter to `CLM_STAT_CD = "CLOSED"`.
If they ask for reopened claims, filter to `CLM_STAT_CD = "RE-OPENED"`.
If they do not specify a claim status, include all CLM_STAT_CD values.
If they ask for a list or listing, only include important columns. 
If they ask to compare different statuses, use several CTEs:
    - CTE to get details of open claims.
    - CTE to get details of closed claims.
    - CTE to compare claims based on their status.
If they ask for a list of submissions, they are asking for a list of claim numbers from the loss run table, possibly filtered by other criteria.
Only calculate a percentage if they ask you to calculate it.

Make sure anything that looks like a date comes back as a date and anything that looks like a number comes back as a number.

{context}

Here are 6 critical rules for the interaction you must abide:
<rules>
1. You MUST MUST wrap the generated sql code within ``` sql code markdown in this format e.g
```sql
(select 1) union (select 2)
```
2. If I don't tell you to find a limited set of results in the sql query or question, you MUST limit the number of responses to 50.
3. Text / string where clauses must be fuzzy match e.g ilike %keyword%
4. Make sure to generate a single snowflake sql code, not multiple. 
5. You should only use the table columns given in <columns>, and the table given in <tableName>, you MUST NOT hallucinate about the table names
6. DO NOT put numerical at the very front of sql variable.
7. DO NOT use DATE_SUB SQL function. Use DATEADD instead.
</rules>

Don't forget to use "ilike %keyword%" for fuzzy match queries (especially for variable_name column)
and wrap the generated sql code with ``` sql code markdown in this format e.g:
```sql
(select 1) union (select 2)
```
The correct function for adding or subtracting a time period from a date is DATEADD. Use DATEADD to handle date adjustments in any SQL queries.
When calculating year-over-year changes, ensure the query handles division by zero. Use a CASE statement to check if the previous year's value is zero and, if so, return NULL or another placeholder instead of performing the division.
When generating SQL queries that calculate averages, make sure to round the result to 2 decimal points. Use the ROUND function in SQL like this: ROUND(AVG(column_name), 2).
When you filter on DATE_OF_LOSS, please include a note in your response that lets the user know that these results are based on the date of loss as we do not have any data indicating the date of payment.
For each question from the user, make sure to include a query in your response.

"""

class ChartDrawer:
    def __init__(self, df):
        self.df = df

    def _check_line_chart_friendly(self, date_threshold=0.9, num_threshold=0.9):
        date_cols = []
        num_cols = []

        for col in self.df.columns:
            # Check for date columns (e.g., claim date)
            valid_dates = pd.to_datetime(self.df[col], errors='coerce').notna().sum()
            if valid_dates / len(self.df) >= date_threshold:
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                date_cols.append(col)
            else:
                # Check for numeric columns (e.g., claim amount)
                valid_nums = pd.to_numeric(self.df[col], errors='coerce').notna().sum()
                if valid_nums / len(self.df) >= num_threshold:
                    self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                    num_cols.append(col)

        # Ensure at least one date column and one numeric column exist
        return len(date_cols) >= 1 and len(num_cols) >= 1, date_cols, num_cols

    @staticmethod
    def is_numeric_series(series):
        try:
            pd.to_numeric(series, errors='raise')
            return True
        except:
            return False

    def _check_bar_chart_friendly(self):
        num_cols = []
        text_cols = []

        for col in self.df.columns:
            if self.is_numeric_series(self.df[col]):
                num_cols.append(col)
            else:
                text_cols.append(col)

        # Ensure we have one text and one numeric column for bar chart
        is_friendly = len(num_cols) == 1 and len(text_cols) == 1
        text_col = text_cols[0] if text_cols else None
        num_col = num_cols[0] if num_cols else None

        return is_friendly, text_col, num_col

    def draw_chart(self):
        line_friendly, date_cols, num_cols = self._check_line_chart_friendly()
        bar_friendly, text_col, num_col = self._check_bar_chart_friendly()

        if line_friendly:
            # Plot a line chart showing claim amounts over time
            st.write("### Line Chart")
            st.line_chart(data=self.df, x=date_cols[0], y=num_cols)
        elif bar_friendly:
            # Ensure correct data types and set the index
            self.df[num_col] = pd.to_numeric(self.df[num_col], errors='coerce')
            self.df = self.df.set_index(text_col)
            
            # Plot the bar chart
            st.write("### Bar Chart")
            st.bar_chart(data=self.df[num_col])
        else:
            st.write("I cannot easily chart this data.")

def get_table_context(_session, table_name: str, table_description: str, metadata_query: str = None):
    """
    Retrieves the table context, including column details and metadata by querying information schema in snowflake.

    Args:
        table_name (str): The name of the table. fully qualified DB.SCHEMA.TABLE
            - FQ Name is split into 3 parts and used to find the right info in information schema.
        table_description (str): The description of the table.
        metadata_query (str, optional): The query to retrieve additional metadata. Defaults to None.

    Returns:
        str: The formatted table context.
    """    
    table = table_name.split(".")
    query_result = _session.sql(f"""
        SELECT COLUMN_NAME, DATA_TYPE FROM {table[0]}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{table[1]}' AND TABLE_NAME = '{table[2]}'
        """,
    ).collect()
    col_names = [f"Column Name: {row['COLUMN_NAME']}; Data Type: {row['DATA_TYPE']}" for row in query_result]
    columns = "\n".join(col_names)
    context = f"""
    Here is the table name <tableName> {table_name} </tableName>

    <tableDescription>{table_description}</tableDescription>

    Here are the columns of the {table_name}

    <columns>\n\n{columns}\n\n</columns>
    """
    
    if metadata_query:
        metadata_result = _session.sql(metadata_query).collect()
        metadata = "\n".join(
            [f"Variable Name: {row['VARIABLE_NAME']}; Definition: {row['DEFINITION']}" for row in metadata_result]
        )
        context = context + f"\n\nAvailable variables by VARIABLE_NAME:\n\n{metadata}"
    return context

def get_system_prompt(session):
    """
    Generates the system prompt.

    Returns:
        str: The generated system prompt.
    """    
    table_context = get_table_context(
        session,
        table_name=QUALIFIED_TABLE_NAME,
        table_description=TABLE_DESCRIPTION,
        metadata_query=METADATA_QUERY
    )
    return GEN_SQL.format(context=table_context)

def main():
    # Get current session
    session = get_active_session()
    
    disclaimer = st.container()
    st.title("🌊❄️🐲 Lake Explorer Nessie")
    
    # Ai intro - self explanatory. This can also be tweaked a bit
    INTRO = """
        Hello! I'm Nessie, your dedicated data assistant here to help you navigate and analyze the vast seas of loss run data in our datalake. 
        Whether you need to pull up specific data, understand trends, or dive deep into analytics, 
        I'm here to guide you every step of the way.

        How can I assist you today?
    """
    
    # On app load - basically if no messages are in the streamlist app yet, send the intro message
    if "messages" not in st.session_state.keys():
        st.session_state.messages = [{"role":"assistant", "content":INTRO}]
    
    # User prompt (st.chat_input() creates the actual input box) then append the user prompt to the streamlit chat session
    if prompt := st.chat_input():
        st.session_state.messages.append({"role":"user", "content": prompt})
    
    # Don't show the system message. Write both the message content and result response.
    for message in st.session_state.messages:
        if message["role"] == "system":
            continue
            
        # Set the avatar for the assistant role
        if message["role"] == "assistant":
            avatar = "🐲"
        else:
            avatar = "🏊‍♂️"

        # Controls message history
        with st.chat_message(message["role"], avatar=avatar):
            if message["role"] == "assistant":
                sql_pattern = re.compile(r"```sql\n(.*)\n```", re.DOTALL)
                sql_match = sql_pattern.search(message["content"])
                
                if sql_match:
                    # Safely access the SQL query if it was found
                    sql = sql_match.group(1)
                    before_sql = message["content"][:sql_match.start()]
                    after_sql = message["content"][sql_match.end()]
            
                    # Display the part before the SQL
                    st.write(before_sql)
            
                    # Use the expander to show the SQL
                    with st.expander("Show SQL Query"):
                        st.code(sql, language="sql")
                    
                    # Display the part after the SQL
                    st.write(after_sql)
                else:
                    # If no SQL was found, display the entire content
                    st.write(message["content"])
            else:
                # For non-assistant messages, just display the content
                st.write(message["content"])
            
            # Display results if available
            if "results" in message:
                st.dataframe(message["results"])
    
    # Try catch: first check if last message was not from assistant. If not then put up a spinner while thinking. 
    try:
        if st.session_state.messages[-1]["role"] != "assistant":
            with st.chat_message("assistant", avatar="🐲"):
                with st.spinner("Thinking..."):
                    # Add the system prompt to the session. Grab the user prompt question from the session state and escape any characters.
                    system_prompt = get_system_prompt(session)
                    safe_system_prompt = system_prompt.replace("'", "''").replace("\\", "\\\\")
                    
                    st.session_state.system_prompt = safe_system_prompt
                    time.sleep(.3)
                    
                    # Grab the user prompt question from the session state and escape any characters.
                    for m in st.session_state.messages:
                        my_q = m["content"]
                    
                    safe_my_q = my_q.replace("'", "''").replace("\\", "\\\\")
    
                    # Ask snowflake cortex the user question. Give it both the system prompt and user prompt.
                    question_response = session.sql(f"""
                    SELECT SNOWFLAKE.CORTEX.COMPLETE(
                        'llama3-70b',
                        [
                            {{'role': 'system', 'content': '{safe_system_prompt}'}},
                            {{'role': 'user', 'content': '{safe_my_q}'}}
                        ], {{}}
                        ) as response;
                    """)
    
                    # Collect the response df, parse and write. Add to message.
                    response = question_response.collect()
                    response_json = json.loads(response[0][0])
                    answer = response_json['choices'][0]['messages']
                    message = {"role": "assistant", "content": answer}
    
                    # Check if there is SQL in the response.
                    sql_match = re.search(r"```sql\n(.*)\n```", answer, re.DOTALL)
    
                    # If there is SQL in the response, parse the SQL to string. Query snowflake. Collect the SQL results df.
                    if sql_match:
                        
                        sql = sql_match.group(1)
                        message["results"] = session.sql(sql).collect()
                        df = message["results"]

                        before_sql = answer[:sql_match.start()]
                        after_sql = answer[sql_match.end():]
                        
                        st.write(before_sql)
                        
                        with st.expander("Show SQL Query"):
                            st.code(sql, language="sql")
                            
                        st.write(after_sql)
                        
                        st.write("### Query Results")
                        st.dataframe(message["results"])

                        ### CHARTING LOGIC: Charts expect a pandas DF
                        df_chart = pd.DataFrame(message["results"])
                        chart_drawer = ChartDrawer(df_chart)
                        chart_drawer.draw_chart()
                    else:
                        st.write(answer)
    
                    # Add the results as a streamlit dataframe UI item. And append this to the response as well.
                    st.session_state.messages.append(message)
                    
    except Exception as e:
        st.error(str(e), icon="🚨")
        message = {"role": "assistant", "content": str(e)}
        st.session_state.messages.append(message)
        
    with disclaimer:
        st.info('Nessie can become confused just like us. If you don\'t like the query she came up with try wording your question a different way to help her better understand what you\'re looking for. And please validate all responses against official sources.', icon="ℹ️")

if __name__ == "__main__":
    main()

