import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

# 1. Get Session
session = get_active_session()

st.title("💰 Gold Layer AI Analyst")
st.write("Ask questions about Customer Monthly Summaries (Debit, Credit, Accounts).")

# 2. Schema Definition (UPDATED: Database = sfl_iceberg, Schema = gold_consumption)
SCHEMA_CONTEXT = """
You are an expert Data Analyst converting English questions into Snowflake SQL.
You have access to 4 Gold Layer tables. 

CRITICAL SCHEMA RULE: 
- You MUST use the fully qualified name: 'sfl_iceberg.gold_consumption.table_name'
- Example: Use 'sfl_iceberg.gold_consumption.glue_monthly_customers', NOT just 'glue_monthly_customers'.

1. Table: sfl_iceberg.gold_consumption.glue_monthly_customers (Alias: c)
   - cust_id (STRING): Primary Key
   - cust_name (STRING)
   - cust_no (STRING)
   - is_retail_cust (STRING): 'Y' or 'N'
   - snp_dt_mth_prtn (DATE): Snapshot Month

2. Table: sfl_iceberg.gold_consumption.glue_monthly_cust_acct (Alias: b)
   - cust_id (STRING): FK to customers
   - acct_id (STRING): FK to accounts
   - snp_dt_mth_prtn (DATE)


3. Table: sfl_iceberg.gold_consumption.glue_monthly_accounts (Alias: a)
   - acct_id (STRING): Primary Key
   - retail_acct (STRING): 'Y' or 'N'
   - is_actv (STRING): 'Y' or 'N'
   - snp_dt_mth_prtn (DATE)
   - product (STRING)
   - sub_product (STRING)
   - holding_type (STRING): 'Sole' or 'Joint'

4. Table: sfl_iceberg.gold_consumption.glue_monthly_transactions (Alias: t)
   - acct_id (STRING): FK to accounts
   - tran_type (STRING): 'Debit' or 'Credit'
   - total_amount (DECIMAL)
   - total_trans_count (INT)
   - snp_dt_mth_prtn (DATE)
   - tran_date (DATE)

CRITICAL JOIN RULES:
- Always join on BOTH the ID column AND 'snp_dt_mth_prtn'.
- Join Path: customers (c) -> cust_acct (b) -> accounts (a) -> transactions (t).

GENERAL RULES:
1. If the user asks for  "Active Accounts", filter a.is_actv='Y'
2. If the user asks for  "Retail Accounts", filter a.is_actv = 'Y' and a.retail_acct = 'Y'
3. If the user asks for  "Business Account" or "Non Retail Account",  filter a.retail_acct = 'N'
4. If the user asks for "Active Retail Customers", filter for customers joined with accounts where a.retail_acct='Y', and a.is_actv='Y'.
5. If the user asks for "Active Customers" please look for customers holding atleast 1 account and a.is_actv='Y'
6. If the user asks for  "Inactive Accounts", filter a.is_actv='N'
7. If the user asks for "Inactive Customers" please look for customers not holding any account or a.is_actv<>'Y'
"""

# 3. Initialize Chat History
if "messages" not in st.session_state:
    st.session_state.messages = []

# 4. Display History (Text + Dataframes)
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        # If this message has a saved dataframe result, display it
        if "results" in message:
            st.dataframe(message["results"])
        # If this message has a chart, display it
        if "chart_data" in message:
             st.bar_chart(message["chart_data"], x=message["chart_x"], y=message["chart_y"])

# 5. Handle User Input
if prompt := st.chat_input("Ex: Show top 5 customers by credit amount in Dec 2025"):
    
    # Show User Message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)


# 6. Generate & Execute
    with st.chat_message("assistant"):
        
        # --- A. Generate SQL ---
        with st.spinner("Generating SQL..."):
            
            # --- NEW: Build Chat History String ---
            # We take the last 5 messages to give context without overloading the token limit
            chat_history_str = ""
            for msg in st.session_state.messages[-5:]:
                role = msg["role"].upper()
                content = msg["content"]
                # We skip the large dataframes, just keep the text/SQL context
                chat_history_str += f"{role}: {content}\n"
            
            # Construct the FULL prompt with history
            full_prompt = f"""
            {SCHEMA_CONTEXT}

            HISTORY OF CONVERSATION:
            {chat_history_str}

            CURRENT QUESTION:
            User: {prompt}

            SQL Query:"""
            
            # Escape quotes for Snowflake SQL string
            safe_prompt = full_prompt.replace("'", "''") 
            
            # Use SQL-based Cortex call
            cortex_query = f"SELECT snowflake.cortex.COMPLETE('mistral-large', '{safe_prompt}')"
            
            try:
                # Run Cortex
                sql_result = session.sql(cortex_query).collect()
                generated_sql = sql_result[0][0]
                
                # Clean Markdown tags
                generated_sql = generated_sql.replace("```sql", "").replace("```", "").strip()
                
                # Display the generated SQL code block
                st.markdown(f"**Generated SQL:**\n```sql\n{generated_sql}\n```")

            except Exception as e:
                st.error("AI Generation Failed.")
                st.stop()

        # --- B. Execute SQL & Show Results ---
        # (This part remains exactly the same as before)
        with st.spinner("Executing Query..."):
            try:
                # Run the generated SQL
                df_result = session.sql(generated_sql).to_pandas()
                
                # Check if we got data
                if df_result.empty:
                    st.warning("The query ran successfully but returned no data.")
                    # Save text-only history
                    st.session_state.messages.append({
                        "role": "assistant", 
                        "content": f"Executed SQL: `{generated_sql}`\n\nResult: No data found."
                    })
                else:
                    st.success(f"Found {len(df_result)} rows.")
                    st.dataframe(df_result)
                    
                    # Prepare message payload for history
                    msg_payload = {
                        "role": "assistant", 
                        "content": f"Executed SQL: `{generated_sql}`",
                        "results": df_result # Save dataframe for history
                    }

                    # --- C. Auto-Chart Logic ---
                    # If small result (<50 rows) and has numbers, try to chart
                    if len(df_result) > 0 and len(df_result) < 50:
                        num_cols = df_result.select_dtypes(include=['float', 'int']).columns
                        txt_cols = df_result.select_dtypes(include=['object', 'string']).columns
                        
                        if len(num_cols) > 0 and len(txt_cols) > 0:
                            st.bar_chart(df_result, x=txt_cols[0], y=num_cols[0])
                            # Save chart settings for history
                            msg_payload["chart_data"] = df_result
                            msg_payload["chart_x"] = txt_cols[0]
                            msg_payload["chart_y"] = num_cols[0]

                    # Save to Session State
                    st.session_state.messages.append(msg_payload)

            except Exception as e:
                st.error(f"SQL Execution Error: {e}")
                st.error(f"Details: {e}")
                st.session_state.messages.append({
                    "role": "assistant", 
                    "content": "I generated invalid SQL. Please try rephrasing."
                })
