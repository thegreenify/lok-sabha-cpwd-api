import json
import os
import boto3
from datetime import datetime
from fpdf import FPDF # fpdf2 library

# Initialize DynamoDB and S3 clients
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

water_bills_table = dynamodb.Table(os.environ['WATER_BILLS_TABLE_NAME'])
allottees_table = dynamodb.Table(os.environ['ALLOTTEES_TABLE_NAME'])
pdf_bills_bucket_name = os.environ['PDF_BILLS_BUCKET_NAME']

class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 15)
        self.cell(0, 10, 'Lok Sabha Quarters - Water Bill', 0, 1, 'C')
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}/{{nb}}', 0, 0, 'C')

    def chapter_title(self, title):
        self.set_font('Arial', 'B', 12)
        self.cell(0, 10, title, 0, 1, 'L')
        self.ln(5)

    def chapter_body(self, body):
        self.set_font('Arial', '', 10)
        self.multi_cell(0, 5, body)
        self.ln()

def lambda_handler(event, context):
    try:
        allottee_id = event['pathParameters'].get('allottee_id')
        billing_month = event['pathParameters'].get('billing_month') # Expected format YYYY-MM

        if not allottee_id or not billing_month:
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'message': 'Allottee ID and Billing Month are required.'})
            }

        # 1. Fetch bill data from DynamoDB
        bill_response = water_bills_table.get_item(
            Key={
                'allottee_id': allottee_id,
                'billing_month': billing_month
            }
        )
        bill_item = bill_response.get('Item')

        if not bill_item:
            return {
                'statusCode': 404,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'message': 'Water bill not found for the specified allottee and month.'})
            }

        # 2. Fetch allottee details (optional, for richer PDF)
        allottee_response = allottees_table.get_item(
            Key={
                'quarter_id': bill_item['quarter_id'] # Assuming quarter_id is primary key
            }
        )
        allottee_details = allottee_response.get('Item', {})

        # 3. Generate PDF
        pdf = PDF()
        pdf.add_page()
        pdf.set_auto_page_break(auto=True, margin=15)

        pdf.chapter_title(f"Bill for {billing_month}")

        # Allottee Details
        pdf.set_font('Arial', 'B', 10)
        pdf.cell(0, 7, 'Allottee Details:', 0, 1)
        pdf.set_font('Arial', '', 10)
        pdf.cell(0, 7, f"Name: {allottee_details.get('name', 'N/A')}", 0, 1)
        pdf.cell(0, 7, f"Employee ID: {allottee_details.get('employee_id', 'N/A')}", 0, 1)
        pdf.cell(0, 7, f"Quarter ID: {bill_item.get('quarter_id', 'N/A')}", 0, 1)
        pdf.ln(5)

        # Bill Details
        pdf.set_font('Arial', 'B', 10)
        pdf.cell(0, 7, 'Bill Summary:', 0, 1)
        pdf.set_font('Arial', '', 10)
        pdf.cell(0, 7, f"Billing Month: {bill_item.get('billing_month', 'N/A')}", 0, 1)
        pdf.cell(0, 7, f"Billed Amount: INR {bill_item.get('amount_inr', '0.00')}", 0, 1)
        pdf.cell(0, 7, f"Bill Status: {bill_item.get('status', 'N/A')}", 0, 1)
        pdf.cell(0, 7, f"Billed Date: {bill_item.get('billed_date', 'N/A').split('T')[0]}", 0, 1) # Extract date part
        pdf.ln(10)

        pdf.set_font('Arial', 'I', 9)
        pdf.multi_cell(0, 5, "Note: This is an auto-generated water bill. For any discrepancies, please contact the DDO office.")

        # Save PDF to BytesIO object
        pdf_output = pdf.output(dest='S').encode('latin-1') # Output as bytes, latin-1 encoding for fpdf2

        # 4. Store PDF in S3
        s3_key = f"bills/{allottee_id}/{billing_month}.pdf"
        s3.put_object(Bucket=pdf_bills_bucket_name, Key=s3_key, Body=pdf_output, ContentType='application/pdf')

        # 5. Return PDF content directly (API Gateway binary support)
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/pdf',
                'Content-Disposition': f'attachment; filename="{allottee_id}_{billing_month}_bill.pdf"'
            },
            'body': pdf_output.decode('latin-1'), # Decode back to string for API Gateway, it handles binary
            'isBase64Encoded': False # fpdf2 output is bytes, but API Gateway expects string for direct binary pass-through
            # if binaryMediaTypes is configured correctly, it will handle the byte stream.
            # For Python, often decoding to latin-1 or base64 encoding is needed if direct byte stream fails.
            # Let's use base64 encoding for robustness.
        }
    except Exception as e:
        print(f"Error generating PDF bill: {e}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'message': 'Internal Server Error', 'error': str(e)})
        }