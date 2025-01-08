import streamlit as st
import pandas as pd
import pyodbc
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from io import BytesIO

st.title("Gainer Pendancy Automailer")

def get_db_connection():
    return pyodbc.connect(
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        r'SERVER=4.240.64.61,1232;'
        r'DATABASE=z_scope;'
        r'UID=Utkrishtsa;'
        r'PWD=AsknSDV*3h9*RFhkR9j73;'
    )

conn = get_db_connection()

# Fetching dropdown data
# Brand Dropdown
brnd = pd.read_sql_query("SELECT vcbrand FROM brand_master", conn)
brand_list = ["Select Brand"] + brnd['vcbrand'].tolist()
brand = st.selectbox(label="Brand", options=brand_list)

# Dealer Dropdown
dealer = pd.read_sql_query("SELECT distinct Dealer FROM locationinfo WHERE brand=?", conn, params=(brand,))
dealer_list = ["Select Dealer"] + dealer['Dealer'].tolist()
Dealer = st.selectbox(label="Select Dealer", options=dealer_list)

# Location Dropdown
location = pd.read_sql_query("SELECT distinct Location FROM locationinfo WHERE brand=? and Dealer=?", conn, params=(brand, Dealer))
location_list = ["Select Location"] + location['Location'].tolist()
Location = st.selectbox(label="Select Location", options=location_list)

# File Uploader for Email List
#Mail_list = st.file_uploader("Upload Mail list", type='xlsx')

# Execute SQL Procedure and Load Data
cursor = conn.cursor()
cursor.execute("exec UAD_Gainer_Pendency_Report_LS")

df = pd.read_sql("""
    SELECT Brand, Dealer, CONCAT(Dealer, '_', Dealer_Location) AS [Dealer to Take Action], 
    CONCAT(Co_Dealer, '_', Co_dealer_Location) AS [Co-Dealer],
    Stage, ISNULL([0-2 hrs], 0) AS [0-2 hrs], ISNULL([2-5 hrs], 0) AS [2-5 hrs],
    ISNULL([5-9 hrs], 0) AS [5-9 hrs], ISNULL([1-2 days], 0) AS [1-2 days], 
    ISNULL([2-4 days], 0) AS [2-4 days], ISNULL([>4 days], 0) AS [>4 days],
    (ISNULL([0-2 hrs], 0) + ISNULL([2-5 hrs], 0) + ISNULL([5-9 hrs], 0) + 
    ISNULL([1-2 days], 0) + ISNULL([2-4 days], 0) + ISNULL([>4 days], 0)) AS Total
    FROM (
        SELECT TBL.brand, TBL.Dealer, TBL.Dealer_Location, tbl.Co_Dealer, tbl.Co_dealer_Location, 
        TBL.STAGE, TBL.responcbucket, SUM(tbl.ordervalue) AS ORDERVALUE
        FROM (
            SELECT brand, Dealer, Dealer_Location, Category, OrderType, Co_Dealer, Co_dealer_Location,
            Dealer_type, qty, POQty, DISCOUNT, MRP, Stage, Response_Time,
            CASE
                WHEN EXC_HOLIDAYS <= 120 THEN '0-2 hrs'
                WHEN EXC_HOLIDAYS <= 300 THEN '2-5 hrs'
                WHEN EXC_HOLIDAYS <= 540 THEN '5-9 hrs'
                WHEN EXC_HOLIDAYS <= 1080 THEN '1-2 days'
                WHEN EXC_HOLIDAYS <= 2160 THEN '2-4 days'
                ELSE '>4 days'
            END AS responcbucket,
            CASE 
                WHEN ISNULL(POQty, 0) = 0 THEN QTY * (100 - DISCOUNT) * MRP / 100
                ELSE POQty * (100 - DISCOUNT) * MRP / 100
            END AS ordervalue
            FROM gainer_pendency_report_test_1
            WHERE Category = 'Spare Part' AND OrderType = 'new' AND Dealer_type = 'Non_Intra' and brand=?
        ) AS TBL
        GROUP BY TBL.brand, TBL.Dealer, TBL.Dealer_Location, TBL.STAGE, TBL.responcbucket, Co_Dealer, Co_dealer_Location
    ) AS TBL2
    PIVOT (
        SUM(TBL2.ORDERVALUE) FOR TBL2.responcbucket IN ([0-2 hrs], [2-5 hrs], [5-9 hrs], [1-2 days], [2-4 days], [>4 days])
    ) AS TB
    WHERE Stage <> 'PO Awaited'
""", conn,params=(brand,))

cursor.close()
conn.close()

# Function to send mail
def Mail():
    #Mail_df = pd.read_excel(r'C:\Users\Admin\Downloads\Book1.xlsx')
    Mail_df = pd.read_csv(r'https://docs.google.com/spreadsheets/d/e/2PACX-1vRDqBXCxlSXSgOHUAUH6rPqtDQ-RWg9f0AOTFJH2-gAGOoJqubSFjGgRsJjmkECWyeWAP65Vx789z6B/pub?gid=1610467454&single=true&output=csv')
    #Mail_df = pd.read_excel(Mail_list)
    Mail_df['unique_dealer'] = Mail_df['Brand'] + "_" + Mail_df['Dealer'] + "_" + Mail_df['Location']
    df['Unque_Dealer'] = df['Brand'] + "_" + df['Dealer to Take Action']
    df['1-2 days>0']  = (df['5-9 hrs']+df['1-2 days']+df['2-4 days']+df['>4 days'])
    Greater_than_zero =   df[df['1-2 days>0']>0]
    merge_df = Greater_than_zero.merge(Mail_df, left_on='Unque_Dealer', right_on='unique_dealer', how='inner')
    #merge_df = df.merge(Mail_df, left_on='Unque_Dealer', right_on='unique_dealer', how='inner')
    Unique_Dealer = merge_df['Unque_Dealer'].unique()

    for dealer in Unique_Dealer:
        #dealer = 'TATA PCBU_AKAR FOURWHEEL_Jaipur_RAJ'
        filtered_df = merge_df[merge_df['unique_dealer'] == dealer]
        ds = filtered_df[filtered_df['Unque_Dealer']== dealer][['Dealer to Take Action','Co-Dealer', 'Stage',
        '0-2 hrs', '2-5 hrs', '5-9 hrs', '1-2 days', '2-4 days', '>4 days','Total']]
        html_table = ds.to_html(index=False, border=1, justify='center')


        if filtered_df.empty:
            print(f"No data found for dealer: {dealer}")
            continue
        to_email = filtered_df['To'].iloc[0] 
        cc_emails = filtered_df['CC'].iloc[0]
    
        cc_emails = cc_emails.replace(' ', '')  
        cc_email_list = cc_emails.split(';') if cc_emails else []
        all_recipients = [to_email] + cc_email_list
        print(f"Sending email to: {dealer,all_recipients}")

        msg = MIMEMultipart("alternative")
        msg["Subject"] = "Response required on Pending Orders_"+dealer
        #msg["From"] = "scsit.db2@sparecare.in"
        msg["From"] = "gainer.alerts@sparecare.in"
        msg["To"]=to_email
        #msg['Cc'] = ','.join(cc_emails)
        msg['Cc']=cc_emails

        #['hanish.khattar@sparecare.in','manish.sharma@sparecare.in','scope@sparecare.in']
        #"idas98728@gmail.com"

        html_content = f"""
        <html>
        <head>
        <style>
        table {{
            border-collapse: collapse;
            width: 100%;
            text-align: center;
        }}
        th, td {{
            border: 1px solid black;
            padding: 8px;
        }}
        th {{
            background-color: #33ffda;
        }}
        body, p, th, td {{
            color: black; }}

        </style>
        </head>
        <body>
        <p style="font-family: 'Calibri', Times, serif;">Dear Sir,</p>
        <p style="font-family: 'Calibri', Times, serif;">Greetings !! </p>
        <p style="font-family: 'Calibri', Times, serif;">As per current transactions status,
        following Orders are showing pending for long time at your dealership.
        </p>

        {html_table}

        <p style="font-family: 'Calibri', Times, serif;">Kindly check & take action at the earliest. Delay in response will affect your <b>RANKING AS SELLER</b> and result in Lesser Liquidation of Non Moving Parts.
        </p>
        <p style ="font-family:'Calibri',Times,serif;">For any issue/support required, please <b>Whatsapp on +91 8882263920</b></p>
        <p style="font-family: 'Calibri', Times, serif;">Warm Regards,<br>Gainer Team</p>


        </body>
        </html>

        """
        msg.attach(MIMEText(html_content, "html"))

        # Send the email
        try:
            with smtplib.SMTP("smtp.gmail.com", 587) as server:
                server.starttls()
                server.login('gainer.alerts@sparecare.in', 'fmyclggqzrmkykol')
                server.sendmail('gainer.alerts@sparecare.in', all_recipients, msg.as_string())
            print("Email sent successfully!")
        except Exception as e:
            print(f"Error: {e}")

    st.success("Emails sent successfully!")

# Function to convert DataFrame to Excel
def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        format1 = workbook.add_format({'num_format': '0.00'})
        worksheet.set_column('A:A', None, format1)
    return output.getvalue()

# Buttons for downloading data and sending mail
col1, col2,col3 = st.columns(3)

with col1:
    if st.button('📊 Generate Data'):
        df_xlsx = to_excel(df)
        st.download_button(
            label="📥 Download Excel File",
            data=df_xlsx,
            file_name=f"{brand}_Pendency_Report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

with col2:
    st.link_button(label="⌨ Google Mail List",url="https://docs.google.com/spreadsheets/d/1UO5pF3yKaYemf-s3YKK62yjbT0zdG4EjTUmlzcQHT00/edit?gid=1610467454#gid=1610467454")
with col3:
    if st.button('📧 Send Mail'):
        Mail()
