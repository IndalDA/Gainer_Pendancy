import streamlit as st
import pandas as pd
import pyodbc
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from io import BytesIO

st.title("Gainer Pendancy Automailer")

tab1,tab2,tab3,tab4 = st.tabs(['Seller Pendancy','Buyer Pendancy','Stock Update','Own Arrange'])

def get_db_connection():
    return pyodbc.connect(
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        r'SERVER=103.234.185.132,2499;'
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

#brnadid
try:
    bigid =pd.read_sql_query("""select bigid from Brand_Master where vcbrand=?""",conn,params=(brand))
    brandid = bigid.iloc[0][0]
#    st.write(brandid)
except:
    pass    

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
# Function to send mail
def Mail(brand):
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
        filtered_df = merge_df[merge_df['unique_dealer'] == dealer]
        ds = filtered_df[filtered_df['Unque_Dealer']== dealer][['Dealer to Take Action','Co-Dealer', 'Stage',
        '0-2 hrs', '2-5 hrs', '5-9 hrs', '1-2 days', '2-4 days', '>4 days','Total']]
        
        #sub = filtered_df['Buyer_Dealer'].values+"_"+filtered_df['Buyer_Location'].values
        #s = "Own Arrangement shipment-"+str(sub).replace("['",'').replace("']",'')
        #subject = str(s)

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
        <p style ="font-family:'Calibri',Times,serif;">For any issue/support required, please write mail to <b>gainer.support@sparecare.in</b></p>
        <p style="font-family: 'Calibri', Times, serif;">Warm Regards,<br>Gainer Team</p>
        
        <p style="font-family: 'Calibri', Times, serif;">This is system generated mail, please do not reply.</p>


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
#col1, col2,col3 = st.columns(3)

#own arrangement

def Own_arrangement_Mail(brandid):
  import pandas as pd
 # from tabulate import tabulate
  from email.mime.multipart import MIMEMultipart
  from email.mime.text import MIMEText
  import smtplib
  import pyodbc

  conn = pyodbc.connect(
      r'DRIVER={ODBC Driver 17 for SQL Server};'
      r'SERVER=103.234.185.132,2499;'
      r'DATABASE=z_scope;'
      r'UID=Utkrishtsa;'
      r'PWD=AsknSDV*3h9*RFhkR9j73;')
  cursor = conn.cursor()

  df = pd.read_sql("""
  select distinct b.Brand ,
  c.Dealer Buyer_Dealer,c.Location as Buyer_Location,a.DispatchOrderNo,b.Dealer SellerDealer,
  b.Location SellerLocation,d.lrnumber,format(a.DISPATCHDATE,'dd-MMM-yyy') DISPATCHDATE,format(d.LRDate,'dd-MMM-yyy') LRDate,
  d.TransporterName,f.InvoiceNumber,f.InvoiceAmount,DATEDIFF(DAY,LRDate,GETDATE()) AgeingDays
  from SH_PartTransaction  a
  Inner join locationinfo b on a.sellerlocation=b.locationid
  Inner join locationinfo c on a.BUYERLOCATION=c.locationid
  inner join SH_DispatchDetail  d on a.DispatchOrderNo=d.DispatchOrderNo
  inner join SH_DispatchInvoiceDetail f on a.DispatchOrderNo=f.DispatchOrderNo
  --left join CompanyMaster    e on d.CompanyCode=e.CompanyCode
  where d.CompanyCode=3 and PARTNUMBER is not null and a.RECEIVEDATE is null
  and c.BrandID=? and DISPATCHDATE <= DATEADD(day,-5,getdate()) and b.Dealer not like '%Test%' and b.DealerID<>c.DealerID
  """,conn,params=(brandid,))

  # MAIL
#  Mail_df = pd.read_excel(r'C:\Users\Admin\Downloads\Book1.xlsx')
  Mail_df = pd.read_csv(r'https://docs.google.com/spreadsheets/d/e/2PACX-1vRDqBXCxlSXSgOHUAUH6rPqtDQ-RWg9f0AOTFJH2-gAGOoJqubSFjGgRsJjmkECWyeWAP65Vx789z6B/pub?gid=1610467454&single=true&output=csv')
  Mail_df['unique_dealer'] = Mail_df['Brand'] + "_" + Mail_df['Dealer'] + "_" + Mail_df['Location']
  df['Unque_Dealer'] = df['Brand']+"_"+df['Buyer_Dealer']+"_"+df['Buyer_Location']
  merge_df = df.merge(Mail_df, left_on='Unque_Dealer', right_on='unique_dealer', how='inner')

  Unique_Dealer = merge_df['Unque_Dealer'].unique()

  sent_df =  merge_df[['SellerDealer','SellerLocation','InvoiceNumber','InvoiceAmount','DISPATCHDATE','lrnumber','TransporterName']]
  sent_df.rename(columns={'SellerDealer':'Seller Name','SellerLocation':'Seller Location','InvoiceNumber':'Invoice Number','InvoiceAmount':'Invoice Value',
                          'DISPATCHDATE':'Shipment Date','lrnumber':'LR/AWB No','TransporterName':'Courier Name'},inplace=True)

  for dealer in Unique_Dealer:
      #dealer = 'TATA PCBU_AKAR FOURWHEEL_Jaipur_RAJ'
      filtered_df = merge_df[merge_df['unique_dealer'] == dealer]
      #sent_df =  filtered_df[['SellerDealer','SellerLocation','InvoiceNumber','InvoiceAmount','DISPATCHDATE','lrnumber','CompanyName']]
      filtered_df.rename(columns={'SellerDealer':'Seller Name','SellerLocation':'Seller Location','InvoiceNumber':'Invoice Number','InvoiceAmount':'Invoice Value',
                          'DISPATCHDATE':'Shipment Date','lrnumber':'LR/AWB No','TransporterName':'Courier Name'},inplace=True)
        
      ds = filtered_df[filtered_df['Unque_Dealer']== dealer][['Seller Name','Seller Location', 'Invoice Number',
        'Invoice Value', 'Shipment Date', 'LR/AWB No', 'Courier Name']]
      html_table = ds.to_html(index=False, border=1, justify='center')
      sub = filtered_df['Buyer_Dealer'].values+"_"+filtered_df['Buyer_Location'].values
      sub = pd.unique(sub)
      s = "Pending Receipt : "+str(sub).replace("['",'').replace("']",'')
      subject = str(s)
    # subject  = "Pending for Receipt Own Arrangement shipment -" +sub

      if filtered_df.empty:
          print(f"No data found for dealer: {dealer}")
          continue
      to_email =to_email = filtered_df['To'].iloc[0] 
      cc_emails =cc_emails = filtered_df['CC'].iloc[0]
      #['hanish.khattar@sparecare.in','manish.sharma@sparecare.in','scope@sparecare.in'] 
      #['scsit.db2@sparecare.in','massage2indal@gmail.com','scope@sparecare.in','manish.sharma@sparecare.in','']
      cc_emails = cc_emails.replace(' ', '')  
      cc_email_list = cc_emails.split(';') if cc_emails else []
      all_recipients = [to_email] + cc_email_list
      print(f"Sending email to: {dealer,all_recipients}")
      msg = MIMEMultipart("alternative")
      msg["Subject"] =subject 
      #"Own Arrangement shipment -"+sub
      msg["From"] = "gainer.alerts@sparecare.in"
      #msg["From"] = "scsit.db2@sparecare.in"
      #msg["To"] = "idas98728@gmail.com"
      msg["To"]=to_email
      msg['Cc']=cc_emails  
    

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
      <p style="font-family: 'Calibri', Times, serif;">Following shipments are sent by Selling Dealer via Own Arrangement is <b>“Pending for Receiving”</b> in Sparecare Gainer Portal.</p>

      {html_table}

      <p style="font-family: 'Calibri', Times, serif;">It is requested to kindly receive these parts in Gainer Portal.</p>
      <p style ="font-family:'Calibri',Times,serif;">In case shipment not received, kindly write mail to gainer.support@sparecare.in</p>
      <p style="font-family: 'Calibri', Times, serif;">Warm Regards,<br>Gainer Team</p>

      <p style="font-family: 'Calibri', Times, serif;">This is system generated mail, please do not reply.</p>
      


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
  
# stock update 

def stock_update_Mail(brandid):
    import pandas as pd
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    import smtplib
    import pyodbc

    conn = pyodbc.connect(
      r'DRIVER={ODBC Driver 17 for SQL Server};'
      r'SERVER=103.234.185.132,2499;'
      r'DATABASE=z_scope;'
      r'UID=Utkrishtsa;'
      r'PWD=AsknSDV*3h9*RFhkR9j73;')
   
    cursor = conn.cursor()
    df = pd.read_sql("""
        select * from (
            select distinct c.brand, c.dealer, c.location, c.DealerID, c.LocationID, 
                   format(a.stockdate, 'dd-MMM-yy') stockdate,
                   DATEDIFF(day, CAST(a.stockdate as date), CAST(getdate() as date)) Day_Difference
            from CurrentStock1 a 
            inner join CurrentStock2 b on a.tcode = b.stockcode
            inner join LocationInfo c on a.LocationID = c.LocationID
            WHERE c.Status = 1 and c.SharingStatus = 1 and c.BrandID = ?) as tbl
        where tbl.Day_Difference >= 5
    """, conn, params=(brandid,))

    # Read email details
    Mail_df = pd.read_csv(r'https://docs.google.com/spreadsheets/d/e/2PACX-1vRDqBXCxlSXSgOHUAUH6rPqtDQ-RWg9f0AOTFJH2-gAGOoJqubSFjGgRsJjmkECWyeWAP65Vx789z6B/pub?gid=997145252&single=true&output=csv')
    Mail_df['unique_dealer'] = Mail_df['Brand'] + "_" + Mail_df['Dealer'] + "_" + Mail_df['Location']
    df['Unque_Dealer'] = df['brand'] + "_" + df['dealer'] + "_" + df['location']

    merge_df = df.merge(Mail_df, left_on='Unque_Dealer', right_on='unique_dealer', how='inner')
    merge_df['Stock_filter']=merge_df['brand']+'_'+merge_df['dealer']
    Unique_Dealer = merge_df['Stock_filter'].unique()

    for dealer in Unique_Dealer:
        filtered_df = merge_df[merge_df['Stock_filter'] == dealer]
        filtered_df.rename(columns={'dealer': 'Dealer Name', 'location': 'Dealer Location', 'stockdate': 'Last Stock Update Date'}, inplace=True)

        ds = filtered_df[filtered_df['Stock_filter'] == dealer][['Dealer Name', 'Dealer Location', 'Last Stock Update Date']]
        html_table = ds.to_html(index=False, border=1, justify='center')
       # sub = filtered_df['Dealer Name'].values + "_" + filtered_df['Dealer Location'].values
        sub = filtered_df['Dealer Name'].unique()
        subject = "Stock Update Status - " + str(sub).replace("['", '').replace("']", '')

        if filtered_df.empty:
            print(f"No data found for dealer: {dealer}")
            continue
        email_set_to = set()
        for email_string in filtered_df['To'].dropna():
            emails = email_string.split(';')
            cleaned_emails = {email.strip() for email in emails}
            email_set_to.update(cleaned_emails)
        unique_email_list_to = sorted(email_set_to)
        
        email_set_CC = set()
        for email_string in filtered_df['CC'].dropna():
            emails = email_string.split(';')
            cleaned_emails = {email.strip() for email in emails}
            email_set_CC.update(cleaned_emails)
        unique_email_list_cc = sorted(email_set_CC)
        
        #all_recipients = [unique_email_list_to] + [unique_email_list_cc]
        #print(f"Sending email to: {dealer,all_recipients}")
            # Flatten recipient lists
        all_recipients = unique_email_list_to + unique_email_list_cc

        print(f"Sending email to: {dealer}, {all_recipients}")

        to_email = ", ".join(unique_email_list_to)
        cc_emails = ", ".join(unique_email_list_cc)

        
       # to_email = filtered_df['To'].iloc[0]
        #cc_emails = filtered_df['CC'].iloc[0].replace(' ', '')
        #cc_email_list = cc_emails.split(';') if cc_emails else []

        # to_email = filtered_df['To'].iloc[0] 
        # cc_emails = filtered_df['CC'].iloc[0]
        # cc_emails = cc_emails.replace(' ', '')  
        # cc_email_list = cc_emails.split(';') if cc_emails else []
        # all_recipients = [to_email] + cc_email_list
        #print(f"Sending email to: {dealer, all_recipients}")
        #to_email = unique_email_list_to
        #cc_emails = unique_email_list_cc
        
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = "gainer.alerts@sparecare.in"
        msg["To"] = to_email
        msg['Cc'] = cc_emails

        html_content = f"""
        <html>
        <head>
        <style>
        table {{
            border-collapse: collapse;
            width: auto;
            table-layout: auto;
            text-align: center;
        }}
        th, td {{
            border: 1px solid black;
            padding: 4px;
            word-wrap: break-word;
        }}
        th {{
            background-color: #33ffda;
        }}
        body, p, th, td {{
            color: black;
        }}
        </style>
        </head>
        <body>
        <p style="font-family: 'Calibri', Times, serif;">Dear Sir,</p>
        <p style="font-family: 'Calibri', Times, serif;">Greetings !!</p>
        <p style="font-family: 'Calibri', Times, serif;">In Sparecare Gainer Portal, Current Spare Parts Stock of below mentioned location is not updated from Last 5 days.</p>
        <p style="font-family: 'Calibri', Times, serif;">It may increase Order Rejections from your dealership and will affect future orders.</p>

        {html_table}

        <p style="font-family: 'Calibri', Times, serif;">Request to kindly update the Current Stock in Gainer Portal on Priority.</p>
        <p style="font-family: 'Calibri', Times, serif;">For any issue/support required, please mail on <b>gainer.suport@sparecare.in</b></p>
        <p style="font-family: 'Calibri', Times, serif;">Warm Regards,<br>Gainer Team</p>
        <p style="font-family: 'Calibri', Times, serif;">Note: This is system generated mail, pl do not reply.</p>
        </body>
        </html>
        """
        msg.attach(MIMEText(html_content, "html"))

        try:
            with smtplib.SMTP("smtp.gmail.com", 587) as server:
                server.starttls()
                server.login('gainer.alerts@sparecare.in', 'fmyclggqzrmkykol')
                server.sendmail('gainer.alerts@sparecare.in', all_recipients, msg.as_string())
            print("Email sent successfully!")
            st.success("Emails sent successfully!")    
        except Exception as e:
            print(f"Error: {e}")
        st.success("Emails sent successfully!"+subject)    

with tab2:
    col1, col2,col3 = st.columns(3)
    with col1:
        if st.button('📊 Generate Pendancy Data'):
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
            df_xlsx = to_excel(df)
            st.download_button(
                label="📥 Download Excel File",
                data=df_xlsx,
                file_name=f"{brand}_Pendency_Report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    
    with col2:
        st.link_button(label="⌨ Google Mail List",url="https://docs.google.com/spreadsheets/d/1UO5pF3yKaYemf-s3YKK62yjbT0zdG4EjTUmlzcQHT00/edit?gid=1610467454#gid=1610467454")
    with col3:
        if st.button('📧 Send Mail'):
            #Mail(brand)
             Mail(brand)   

with tab4:
    col1, col2,col3 = st.columns(3)
    with col2:
        if st.button("Sent own arrangement mail"):
            brandid = str(brandid)    
            Own_arrangement_Mail(brandid)    
    with col1:
         if st.button('📥 Generate Own Arrangement Report'):
            brandid = str(brandid) 
            df = pd.read_sql("""
                select distinct b.Brand ,
                c.Dealer Buyer_Dealer,c.Location as Buyer_Location,a.DispatchOrderNo,b.Dealer SellerDealer,
                b.Location SellerLocation,d.lrnumber,format(a.DISPATCHDATE,'dd-MMM-yyy') DISPATCHDATE,format(d.LRDate,'dd-MMM-yyy') LRDate,
                d.TransporterName,f.InvoiceNumber,f.InvoiceAmount,DATEDIFF(DAY,LRDate,GETDATE()) AgeingDays
            from SH_PartTransaction  a
            Inner join locationinfo b on a.sellerlocation=b.locationid
            Inner join locationinfo c on a.BUYERLOCATION=c.locationid
            inner join SH_DispatchDetail  d on a.DispatchOrderNo=d.DispatchOrderNo
            inner join SH_DispatchInvoiceDetail f on a.DispatchOrderNo=f.DispatchOrderNo
            --left join CompanyMaster    e on d.CompanyCode=e.CompanyCode
            where d.CompanyCode=3 and PARTNUMBER is not null and a.RECEIVEDATE is null
            and c.BrandID=? and DISPATCHDATE <= DATEADD(day,-5,getdate()) and b.Dealer not like '%Test%'  and b.DealerID<>c.DealerID
            """,conn,params=(brandid,)) 
                    
            df_xlsx = to_excel(df)
            st.download_button(
                label="Download Excel File",
                data=df_xlsx,
                file_name=f"{brand}_Own_Report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

with tab3:
    col1, col2,col3 = st.columns(3)
    with col2:
        if st.button("Sent stock update mail"):
            brandid = str(brandid)    
            stock_update_Mail(brandid)    
    with col1:
         if st.button('📥 Generate Stock Update Report'):
            brandid = str(brandid) 
            df = pd.read_sql("""
                    select *from (
                        select  distinct c.brand,c.dealer, c.location,c.DealerID,c.LocationID,format(a.stockdate,'dd-MMM-yy') stockdate,
                        DATEDIFF(day,CAST(a.stockdate as date),CAST(getdate() as date) )Day_Difference
                        from CurrentStock1 a 
                        inner join CurrentStock2 b on a.tcode=b.stockcode
                        inner join LocationInfo c on a.LocationID=c.LocationID
                        WHERE c.Status=1 and c.SharingStatus=1  and c.BrandID=?) as tbl
                        where tbl.Day_Difference>=5 
                        """,conn,params=(brandid,))
             
            Mail_df = pd.read_csv(r'https://docs.google.com/spreadsheets/d/e/2PACX-1vRDqBXCxlSXSgOHUAUH6rPqtDQ-RWg9f0AOTFJH2-gAGOoJqubSFjGgRsJjmkECWyeWAP65Vx789z6B/pub?gid=997145252&single=true&output=csv')
            Mail_df['unique_dealer'] = Mail_df['Brand'] + "_" + Mail_df['Dealer'] + "_" + Mail_df['Location']
            df['Unque_Dealer'] = df['brand'] + "_" + df['dealer'] + "_" + df['location']
            merge_df = df.merge(Mail_df, left_on='Unque_Dealer', right_on='unique_dealer', how='left')
            df_xlsx = to_excel(merge_df)
            st.download_button(
                label="Download Excel File",
                data=df_xlsx,
                file_name=f"{brand}_Stock_update_Report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    

# Automailer for PO Sta
def Po_stage_pendancy(brandid):
    import pandas as pd
    from tabulate import tabulate
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    import smtplib
    import pyodbc
    
    conn = pyodbc.connect(
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        r'SERVER=103.234.185.132,2499;'
        r'DATABASE=z_scope;'
        r'UID=Utkrishtsa;'
        r'PWD=AsknSDV*3h9*RFhkR9j73;')
    cursor = conn.cursor()
    cursor.execute("exec UAD_Gainer_Pendency_Report_LS")
    
    df = pd.read_sql("""
    select  Brand,Dealer,concat(dealer,'_',dealer_location)[Dealer to Take Action],concat(Co_Dealer,'_',Co_dealer_Location)[Co-Dealer]
    ,Stage,isnull([0-2 hrs],0)[0-2 hrs],isnull([2-5 hrs],0)[2-5 hrs],
     isnull([5-9 hrs],0)[5-9 hrs],isnull([1-2 days],0)[1-2 days],isnull([2-4 days],0)[2-4 days],isnull([>4 days],0)[>4 days],
    (isnull([0-2 hrs],0)+isnull([2-5 hrs],0)+isnull([5-9 hrs],0)+isnull([1-2 days],0)+isnull([2-4 days],0)+isnull([>4 days],0)) Total
    from(
    SELECT TBL.brand,TBL.Dealer,TBL.Dealer_Location,tbl.Co_Dealer,tbl.Co_dealer_Location,TBL.STAGE,TBL.responcbucket,SUM(tbl.ordervalue) ORDERVALUE
    FROM(
    select brand,Dealer,Dealer_Location, Category,OrderType,Co_Dealer,Co_dealer_Location
    ,Dealer_type
    ,qty,POQty,DISCOUNT,MRP,Stage,
    Response_Time,
    CASE
    WHEN EXC_HOLIDAYS <= 120 THEN '0-2 hrs'
    WHEN EXC_HOLIDAYS <= 300 THEN '2-5 hrs'
    WHEN EXC_HOLIDAYS <= 540 THEN '5-9 hrs'
    WHEN EXC_HOLIDAYS <= 1080 THEN '1-2 days'
    WHEN EXC_HOLIDAYS <= 2160 THEN '2-4 days'
    ELSE '>4 days'
     end responcbucket,
     case when ISNULL(POQty,0)=0  then QTY*(100-DISCOUNT)*MRP/100 else poqty*(100-DISCOUNT)*MRP/100 end ordervalue
     from gainer_pendency_report_test_1
     where Category='Spare Part' and OrderType='new' and Dealer_type ='Non_Intra'    and BrandID=28
      ) as TBL
      GROUP BY TBL.brand,TBL.Dealer,TBL.Dealer_Location,TBL.STAGE,TBL.responcbucket ,Co_Dealer,Co_dealer_Location) AS TBL2
     PIVOT (SUM(TBL2.ORDERVALUE)
     FOR TBL2.responcbucket IN ([0-2 hrs],[2-5 hrs],[5-9 hrs],[1-2 days],[2-4 days],[>4 days])
     ) AS TB
     where Stage='PO Awaited'
    """,conn)
    
    # MAIL
    Mail_df = pd.read_excel(r"C:\Users\Admin\Downloads\Gainer Mail list for Pendancy.xlsx")
    Mail_df['unique_dealer'] = Mail_df['Brand'] + "_" + Mail_df['Dealer'] + "_" + Mail_df['Location']
    df['Unque_Dealer'] = df['Brand'] + "_" + df['Dealer to Take Action']
    df['1-2 days>0']  = (df['5-9 hrs']+df['1-2 days']+df['2-4 days']+df['>4 days'])
    Greater_than_zero =   df[df['1-2 days>0']>0]
    
    merge_df = Greater_than_zero.merge(Mail_df, left_on='Unque_Dealer', right_on='unique_dealer', how='inner')
    
    Unique_Dealer = merge_df['Unque_Dealer'].unique()
    
    for dealer in Unique_Dealer:
    #    dealer = 'TATA PCBU_AKAR FOURWHEEL_Jaipur_RAJ'
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
        msg["Subject"] = "Pending PO Confirmation - "+dealer
        #msg["From"] = "scsit.db2@sparecare.in"
        msg["From"] = "gainer.alerts@sparecare.in"
        msg["To"]=to_email
        msg['Cc'] = ','.join(cc_emails)
        #msg['Cc']=cc_emails
    
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
        <p style="font-family: 'Calibri', Times, serif;">As per current status, you have received confirmation of Part availability in 
        Gainer Portal from Selling dealer against Order Query raised by your dealership.
        </p>
        <p style="font-family: 'Calibri', Times, serif;">Kindly create the Purchase Order and confirm <b><u>PO Number </u></b>in Sparecare Gainer Portal at the earliest..
        </p>
        {html_table}
        <p style ="font-family:'Calibri',Times,serif;"><b>Note :-</b> Delay in PO confirmation may result in loss of Part Availability, as parts could be sold to other party by Seller.</b></p>
        <p style="font-family: 'Calibri', Times, serif;">For any issue/support required, please revert.</p>
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
with tab2:
    col1, col2,col3 = st.columns(3)
    with col1:
        if st.button('📊 Generate Po Stage Report'):
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
            WHERE Stage ='PO Awaited'
        """, conn,params=(brand,))
    
            Mail_df = pd.read_csv(r'https://docs.google.com/spreadsheets/d/e/2PACX-1vRDqBXCxlSXSgOHUAUH6rPqtDQ-RWg9f0AOTFJH2-gAGOoJqubSFjGgRsJjmkECWyeWAP65Vx789z6B/pub?gid=997145252&single=true&output=csv')
            Mail_df['unique_dealer'] = Mail_df['Brand'] + "_" + Mail_df['Dealer'] + "_" + Mail_df['Location']
            df['Unque_Dealer'] = df['brand'] + "_" + df['dealer'] + "_" + df['location']
            merge_df = df.merge(Mail_df, left_on='Unque_Dealer', right_on='unique_dealer', how='left')
            df_xlsx = to_excel(merge_df)
            st.download_button(
                label="Download Excel File",
                data=df_xlsx,
                file_name=f"{brand}_Po_Stage_Pendancy_Report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
       with col2:
        if st.button("Sent Po Pendancy Report"):
            brandid = str(brandid)
            Po_stage_pendancy(brandid)    


cursor.close()
conn.close()
