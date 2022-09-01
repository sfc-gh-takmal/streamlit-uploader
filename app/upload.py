from datetime import datetime
import pandas as pd 
import streamlit as st
from snowflake.snowpark import Session


#Boilerplate code to create a Snowpark Session
@st.experimental_singleton # magic to cache db connection
def create_snowpark_session(connection_parameters):
    session = Session.builder.configs(connection_parameters).create()
    return session

connection_parameters = {
    "account": st.secrets["account"],
    "user": st.secrets["user"],
    "password": st.secrets["password"],
    "role": st.secrets["role"],
    "warehouse": st.secrets["warehouse"],
    "database": st.secrets["database"],
    "schema": st.secrets["schema"]
  }

session = create_snowpark_session(connection_parameters)


# set up a Snowpark data frame for a specific Store
my_snowpark_dataframe = session.table('WAREHOUSE_DELIVERY')

# Create a Webpage title with logo

col1, col2, col3 = st.columns([1,3,20])
with col1:
    #Insert logo image
    st.image('coke_consolidated.png', width=115)
with col3:
    st.title('Coke Consolidated')

##USING THIS AS A PLACE HOLDER FOR METRICS
#col1, col2, col3, col4 = st.columns(4)
#with col1:
#    st.metric(label="Updates Today", value="2")

#with col2:
#    st.metric(label="Updates This Month", value="10")

#with col3:
#    st.metric(label="Updates This Year", value="150")

#with col4:
#    st.metric(label="Total Updates", value="1000")


#st.dataframe(my_snowpark_dataframe.to_pandas())


@st.cache
def populate_dropdown(column):
    return list(my_snowpark_dataframe[[column]].distinct().sort(column,ascending=True).to_pandas()[column])

@st.cache
def insert_comment(warehouse_id,date,comment):
    key = str(date)+'|'+str(v_warehouse)

    session.create_dataframe(data=[[warehouse_id,date,comment]],schema=["warehouse_id","date","comment"]).write.mode("append").save_as_table('delivery_comments')
    return None

#Creating a form for data insert
form = st.form(key="annotation",clear_on_submit=True)
with form:
    cols = st.columns((1, 1))
    
    #Dropdown for warehouse selection
    warehouse_list = populate_dropdown('WAREHOUSE_ID')
    v_warehouse = cols[0].selectbox('Warehouse ID: ',warehouse_list)

    #Reasons dropdown
    reason = cols[1].selectbox(
        "Reason:", ["Asked for New Date", "Could Not Drop Off", "Late", "Cancelled", ""], index=4
    )

    #Inserting comments into snowflake
    v_comment = st.text_input('Comment:')
    comment_df = session.table('DELIVERY_DATA.PUBLIC.DELIVERY_COMMENTS') 

    #Calendar widget to choose date    
    cols = st.columns(2)
    u_date = cols[0].date_input("Update to:")
    submitted = st.form_submit_button(label="Submit")

    #Submit timestamp
    if submitted:
      date = datetime.now()#.strftime("%d/%m/%Y %H:%M:%S")
      insert_comment(v_warehouse,date,v_comment)
      #st.success("Thanks! Your update was recorded.")
      

st.write("#### Updated Warehouses")
st.dataframe(comment_df.to_pandas())