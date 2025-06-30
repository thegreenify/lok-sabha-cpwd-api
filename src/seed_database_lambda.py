import json
import os
import boto3
from datetime import datetime, timedelta

dynamodb = boto3.resource('dynamodb')

allottees_table = dynamodb.Table(os.environ['ALLOTTEES_TABLE_NAME'])
water_bills_table = dynamodb.Table(os.environ['WATER_BILLS_TABLE_NAME'])
payment_statuses_table = dynamodb.Table(os.environ['PAYMENT_STATUSES_TABLE_NAME'])

def seed_allottees():
    allottees_data = [
        {"allottee_id": "LSQA001", "employee_id": "PFMS10001", "name": "Priya Sharma", "quarter_id": "LSL-C-101", "allotment_start_date": "2023-01-01", "status": "OCCUPIED"},
        {"allottee_id": "LSQA002", "employee_id": "PFMS10002", "name": "Rahul Kumar", "quarter_id": "LSL-C-102", "allotment_start_date": "2023-02-15", "status": "OCCUPIED"},
        {"allottee_id": "LSQA003", "employee_id": "PFMS10003", "name": "Anjali Singh", "quarter_id": "LSL-C-103", "allotment_start_date": "2023-03-01", "status": "OCCUPIED"},
        {"allottee_id": "LSQA004", "employee_id": "PFMS10004", "name": "Vikram Yadav", "quarter_id": "LSL-C-104", "allotment_start_date": "2023-04-10", "status": "OCCUPIED"},
        {"allottee_id": "LSQA005", "employee_id": "PFMS10005", "name": "Sneha Gupta", "quarter_id": "LSL-C-105", "allotment_start_date": "2023-05-01", "status": "OCCUPIED"},
        {"allottee_id": "LSQA006", "employee_id": "PFMS10006", "name": "Deepak Verma", "quarter_id": "LSL-C-106", "allotment_start_date": "2023-06-20", "status": "OCCUPIED"},
        {"allottee_id": "LSQA007", "employee_id": "PFMS10007", "name": "Pooja Devi", "quarter_id": "LSL-C-107", "allotment_start_date": "2023-07-01", "status": "OCCUPIED"},
        {"allottee_id": "LSQA008", "employee_id": "PFMS10008", "name": "Sanjay Mishra", "quarter_id": "LSL-C-108", "allotment_start_date": "2023-08-10", "status": "OCCUPIED"},
        {"allottee_id": "LSQA009", "employee_id": "PFMS10009", "name": "Kavita Sharma", "quarter_id": "LSL-C-109", "allotment_start_date": "2023-09-01", "status": "OCCUPIED"},
        {"allottee_id": "LSQA010", "employee_id": "PFMS10010", "name": "Ravi Kumar", "quarter_id": "LSL-C-110", "allotment_start_date": "2023-10-15", "status": "OCCUPIED"}
    ]

    for data in allottees_data:
        # Using update_item to be idempotent and only create if not exists
        allottees_table.put_item(
            Item={
                'quarter_id': data['quarter_id'],
                'allottee_id': data['allottee_id'],
                'employee_id': data['employee_id'],
                'name': data['name'],
                'allotment_start_date': data['allotment_start_date'],
                'allotment_end_date': data.get('allotment_end_date'),
                'status': data['status'],
                'last_updated': datetime.now().isoformat() + 'Z'
            }
        )
    print(f"Seeded {len(allottees_data)} allottee records.")

def seed_bills_and_payments():
    today = datetime.now()
    # Seed bills for the last 3 months for each allottee
    for i in range(1, 11): # For LSQA001 to LSQA010
        allottee_id = f"LSQA{i:03d}"
        employee_id = f"PFMS100{i:02d}"
        quarter_id = f"LSL-C-1{i:02d}"

        for j in range(3): # Last 3 months
            bill_date = today - timedelta(days=j * 30)
            billing_month = bill_date.strftime('%Y-%m')

            amount = 500 + (i * 10) + (j * 5) # Varying amounts

            # Seed Water Bill
            water_bills_table.put_item(
                Item={
                    'allottee_id': allottee_id,
                    'billing_month': billing_month,
                    'quarter_id': quarter_id,
                    'employee_id': employee_id,
                    'amount_inr': amount,
                    'billed_date': bill_date.isoformat() + 'Z',
                    'status': 'PENDING_DDO_UPLOAD'
                }
            )

            # Seed Payment Status (assume all paid except for current month's bill)
            if j > 0: # Assume previous months are paid
                payment_statuses_table.put_item(
                    Item={
                        'employee_id': employee_id,
                        'billing_month': billing_month,
                        'amount_deducted_inr': amount,
                        'status': 'SUCCESS',
                        'confirmed_at': (bill_date + timedelta(days=5)).isoformat() + 'Z'
                    }
                )
    print("Seeded dummy bill and payment records.")

def lambda_handler(event, context):
    # This Lambda is typically triggered by a Custom Resource in CloudFormation
    # It should respond to CloudFormation to indicate success/failure

    response_data = {}
    try:
        request_type = event['RequestType']
        if request_type == 'Create' or request_type == 'Update':
            print("Seeding database...")
            seed_allottees()
            seed_bills_and_payments()
            response_data['Message'] = "Database seeded successfully."
        elif request_type == 'Delete':
            print("Delete event received (no specific cleanup for seed data needed).")
            response_data['Message'] = "Delete event processed."

        # Send success signal to CloudFormation
        send_response(event, context, 'SUCCESS', response_data)

    except Exception as e:
        print(f"Error seeding database: {e}")
        response_data['Message'] = f"Failed to seed database: {str(e)}"
        send_response(event, context, 'FAILED', response_data)

# Helper function to send response to CloudFormation (required for Custom Resources)
def send_response(event, context, response_status, response_data):
    response_body = json.dumps({
        'Status': response_status,
        'Reason': response_data.get('Message', 'See CloudWatch logs for details'),
        'PhysicalResourceId': context.log_stream_name,
        'StackId': event['StackId'],
        'RequestId': event['RequestId'],
        'LogicalResourceId': event['LogicalResourceId'],
        'Data': response_data
    })

    print(f"Response URL: {event['ResponseURL']}")
    print(f"Response Body: {response_body}")

    headers = {
        'content-type': '',
        'content-length': str(len(response_body))
    }

    try:
        response = requests.put(event['ResponseURL'], headers=headers, data=response_body)
        print(f"Status code: {response.status_code}")
    except Exception as e:
        print(f"send_response failed: {e}")

# This import is needed for the send_response helper function, but 'requests'
# is not typically included in Lambda's default environment.
# Make sure 'requests' is in your requirements.txt
import requests