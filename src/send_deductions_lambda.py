import json
import boto3
import os
from datetime import datetime, timedelta
import csv
from io import StringIO
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# Initialize DynamoDB clients
dynamodb = boto3.resource('dynamodb')
allottees_table = dynamodb.Table(os.environ['ALLOTTEES_TABLE_NAME'])
water_bills_table = dynamodb.Table(os.environ['WATER_BILLS_TABLE_NAME'])

# Initialize SES client
ses_client = boto3.client('ses')

def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event)}")

    ddo_email_recipient = os.environ.get('DDO_EMAIL_RECIPIENT')
    ses_email_sender = os.environ.get('SES_EMAIL_SENDER')

    if not ddo_email_recipient or not ses_email_sender:
        print("DDO Email Recipient or SES Sender Email not configured.")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Email configuration missing.'})
        }

    try:
        # Determine the billing month (e.g., previous month)
        current_date = datetime.now()
        billing_month_dt = current_date.replace(day=1) - timedelta(days=1) # Last day of previous month
        billing_month = billing_month_dt.strftime('%Y-%m')

        deduction_data = [] # Data to be written to CSV

        # CSV Header
        csv_header = ["EMPLOYEE_ID", "ALLOTTEE_ID", "QUARTER_ID", "BILLING_MONTH", "AMOUNT_INR", "REASON"]
        deduction_data.append(csv_header)

        # 1. Fetch all relevant allottees for the billing month
        # This logic needs to be robust:
        # - Iterate through all recorded water consumption for the month from MDMS (not directly in this code)
        # - For each consumption record, map it to the allottee occupying the quarter during that period.
        # - Calculate the bill amount.

        # For this example, we'll iterate through allottees and generate dummy bills based on their status
        scan_response = allottees_table.scan() # Caution: Avoid scan for very large tables in prod
        allottees = scan_response.get('Items', [])

        for allottee in allottees:
            quarter_id = allottee['quarter_id']
            employee_id = allottee.get('employee_id')
            allottee_id = allottee.get('allottee_id')
            status = allottee.get('status')
            allotment_start_date_str = allottee.get('allotment_start_date')
            allotment_end_date_str = allottee.get('allotment_end_date')

            if not employee_id: # Skip if no employee associated
                continue

            # Check if allottee was occupying the quarter during the billing month
            # This is simplified. Actual logic should use meter readings and occupancy dates.
            is_occupied_during_month = False
            if status == "OCCUPIED" and (not allotment_start_date_str or datetime.strptime(allotment_start_date_str, '%Y-%m-%d').strftime('%Y-%m') <= billing_month):
                is_occupied_during_month = True
            elif status in ["VACATED", "TRANSFERRED"] and allotment_end_date_str:
                if datetime.strptime(allotment_start_date_str, '%Y-%m-%d').strftime('%Y-%m') <= billing_month and \
                        datetime.strptime(allotment_end_date_str, '%Y-%m-%d').strftime('%Y-%m') >= billing_month_dt.strftime('%Y-%m'):
                    is_occupied_during_month = True # For pro-rata billing in vacation/transfer month

            if not is_occupied_during_month:
                print(f"Quarter {quarter_id} not occupied by {allottee_id} during {billing_month}. Skipping.")
                continue

            # --- Mock Water Charge Calculation ---
            # In a real system:
            # 1. Retrieve meter readings for quarter_id for billing_month from MDMS.
            # 2. Calculate consumption based on start/end readings for the occupancy period.
            # 3. Apply DoE rates to get amount.
            water_charge_amount = 500.00 + (len(quarter_id) % 5) * 10.0 # Just a dummy calculation for demonstration

            # Store the generated bill in WaterBillsTable
            water_bills_table.put_item(
                Item={
                    'allottee_id': allottee_id,
                    'billing_month': billing_month,
                    'quarter_id': quarter_id,
                    'employee_id': employee_id,
                    'amount_inr': water_charge_amount,
                    'billed_date': datetime.now().isoformat() + 'Z',
                    'status': 'PENDING_DDO_UPLOAD' # New status indicating it's sent to DDO
                }
            )

            deduction_data.append([
                employee_id,
                allottee_id,
                quarter_id,
                billing_month,
                str(water_charge_amount), # Convert to string for CSV
                f"Water Charges - {billing_month}"
            ])

        if not deduction_data[1:]: # Check if there are actual data rows besides header
            print(f"No deduction data generated for {billing_month}. Email will not be sent.")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': f'No deduction data generated for {billing_month}.'})
            }

        # 2. Generate CSV file in memory
        csv_buffer = StringIO()
        csv_writer = csv.writer(csv_buffer)
        csv_writer.writerows(deduction_data)
        csv_content = csv_buffer.getvalue()

        # 3. Send email with CSV attachment via SES
        msg = MIMEMultipart()
        msg['Subject'] = f"Lok Sabha Quarters - Water Charges for {billing_month}"
        msg['From'] = ses_email_sender
        msg['To'] = ddo_email_recipient

        # Email body
        body_text = f"""Dear DDO,

Please find attached the monthly water charge deduction data for Lok Sabha Quarters for the month of {billing_month}.

This data is to be uploaded to PFMS EIS module using COMPDDO for direct salary deductions.

Total entries: {len(deduction_data) - 1}

Regards,
Lok Sabha Water Billing System
"""
        msg.attach(MIMEText(body_text, 'plain'))

        # Attach CSV file
        filename = f"LokSabhaWaterCharges_{billing_month}.csv"
        part = MIMEApplication(csv_content.encode('utf-8'))
        part.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(part)

        ses_client.send_raw_email(
            Source=ses_email_sender,
            Destinations=[ddo_email_recipient],
            RawMessage={'Data': msg.as_string()}
        )

        print(f"Successfully sent water charge data email to DDO for {billing_month}.")
        return {
            'statusCode': 200,
            'body': json.dumps({'message': f'Water charge data sent to DDO email for {billing_month}.'})
        }

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Internal Server Error', 'error': str(e)})
        }