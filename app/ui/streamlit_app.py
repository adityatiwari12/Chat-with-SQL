import streamlit as st
import requests
import pandas as pd
import time

# Configuration
API_URL = "http://localhost:8001"

st.set_page_config(
    page_title="Chat with SQL",
    page_icon="💬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .stChatMessage {
        border-radius: 10px;
        padding: 10px;
        margin-bottom: 10px;
    }
    .stChatMessage.user {
        background-color: #f0f2f6;
    }
    .stChatMessage.assistant {
        background-color: #e6f7ff;
    }
    .status-box {
        padding: 8px;
        border-radius: 5px;
        margin-bottom: 5px;
        font-size: 0.9em;
    }
    .status-ok {
        background-color: #d4edda;
        color: #155724;
    }
    .status-error {
        background-color: #f8d7da;
        color: #721c24;
    }
    .metric-card {
        background-color: #ffffff;
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 15px;
        text-align: center;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
</style>
""", unsafe_allow_html=True)

# Session State
if "messages" not in st.session_state:
    st.session_state.messages = []
if "selected_table" not in st.session_state:
    st.session_state.selected_table = None

# API Functions
def check_api_health():
    try:
        response = requests.get(f"{API_URL}/health", timeout=2)
        if response.status_code == 200:
            return response.json()
    except:
        return None

def get_tables():
    try:
        response = requests.get(f"{API_URL}/schema-preview", timeout=2)
        if response.status_code == 200:
            return response.json().get("tables", [])
    except:
        return []

def get_table_data(table_name, limit=50):
    try:
        response = requests.get(f"{API_URL}/tables/{table_name}/data", params={"limit": limit}, timeout=5)
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": response.text}
    except Exception as e:
        return {"error": str(e)}

def ask_question(question):
    try:
        response = requests.post(f"{API_URL}/ask", json={"question": question}, timeout=30)
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": response.text}
    except Exception as e:
        return {"error": str(e)}

# Sidebar Navigation & Status
with st.sidebar:
    st.title("🗄️ SQL Assistant")
    
    page = st.radio("Navigation", ["💬 Chat Agent", "📊 Database Explorer"], index=0)
    
    st.divider()
    
    st.subheader("System Status")
    health = check_api_health()
    
    if health and health.get("status") == "ok":
        st.markdown('<div class="status-box status-ok">✅ Backend Online</div>', unsafe_allow_html=True)
        
        with st.expander("System Details"):
            st.write(f"**LLM**: {', '.join(health.get('models_available', []))}")
            st.write(f"**Database**: {'Connected' if health.get('db') else 'Disconnected'}")
    else:
        st.markdown('<div class="status-box status-error">❌ Backend Offline</div>', unsafe_allow_html=True)
        st.caption("Please start the API server.")
        if st.button("Retry Connection"):
            st.rerun()

# Page: Database Explorer
if page == "📊 Database Explorer":
    st.title("📊 Database Explorer")
    st.caption("Inspect your database schema and live data.")
    
    tables_info = get_tables()
    
    if not tables_info:
        st.warning("No tables found or API is offline.")
    else:
        # Table Selection
        table_names = [t['table_name'] for t in tables_info]
        selected = st.selectbox("Select Table", table_names, index=0 if table_names else None)
        
        if selected:
            st.subheader(f"Table: `{selected}`")
            
            # Fetch Data
            with st.spinner("Fetching data..."):
                data_result = get_table_data(selected, limit=100)
            
            if "error" in data_result:
                st.error(f"Failed to load data: {data_result['error']}")
            else:
                # Stats
                cols = st.columns(3)
                cols[0].metric("Total Rows", data_result.get("row_count", "N/A"))
                cols[1].metric("Columns", len(data_result.get("columns", [])))
                
                # Dataframe
                rows = data_result.get("rows", [])
                columns = data_result.get("columns", [])
                
                if rows and columns:
                    df = pd.DataFrame(rows, columns=columns)
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.info("Table is empty.")

# Page: Chat Agent
elif page == "💬 Chat Agent":
    st.title("💬 Chat with SQL")
    st.caption("Ask questions in natural language. The agent will query the database for you.")
    
    # Chat History
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            
            # Show details if available
            if "details" in msg:
                with st.expander("🛠️ SQL & Execution Details"):
                    st.code(msg["details"]["sql"], language="sql")
                    st.write(f"⏱️ **Time:** {msg['details']['time_ms']:.2f} ms")
                    
                    # reconstruct dataframe for history if data exists
                    if msg["details"].get("data") and msg["details"].get("columns"):
                        df = pd.DataFrame(msg["details"]["data"], columns=msg["details"]["columns"])
                        st.dataframe(df, hide_index=True)
    
    # Input
    if prompt := st.chat_input("Ask a question (e.g., 'Top 5 customers by spend')"):
        # Add User Message
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
            
        # Generate Response
        with st.chat_message("assistant"):
            with st.status("Thinking...", expanded=True) as status:
                st.write("Analyzing request...")
                result = ask_question(prompt)
                
                if "error" in result:
                    status.update(label="Error", state="error", expanded=True)
                    st.error(result["error"])
                    st.session_state.messages.append({"role": "assistant", "content": f"Error: {result['error']}"})
                else:
                    status.update(label="Complete", state="complete", expanded=False)
                    
                    answer = result.get("answer", "No answer generated.")
                    st.markdown(answer)
                    
                    # Extract Data for Display
                    query_result = result.get("query_result", {})
                    rows = query_result.get("rows", [])
                    columns = query_result.get("columns", [])
                    sql = result.get("generated_sql", "-- No SQL")
                    time_ms = result.get("total_time_ms", 0)
                    
                    # Show Details Immediately
                    with st.expander("🛠️ SQL & Execution Details"):
                        st.code(sql, language="sql")
                        st.write(f"⏱️ **Time:** {time_ms:.2f} ms")
                        
                        if rows and columns:
                            df = pd.DataFrame(rows, columns=columns)
                            st.dataframe(df, hide_index=True)
                        else:
                            st.write("No data returned.")
                            
                    # Save to History
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": answer,
                        "details": {
                            "sql": sql,
                            "time_ms": time_ms,
                            "data": rows,
                            "columns": columns
                        }
                    })
