import json
import boto3
import os
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
allottees_table = dynamodb.Table(os.environ['ALLOTTEES_TABLE_NAME'])

def lambda_handler(event, context):
    http_method = event['httpMethod']
    path = event['path']

    if http_method == 'GET' and path == '/v1/allottees':
        # Logic for GET /v1/allottees (Billing Software pulling data from CPWD)
        # In a real scenario, this Lambda would *call CPWD's API* to get the data
        # For this SAM template, we're assuming CPWD pushes to us or we're mocking this part.
        # For a full system, you'd likely have a separate internal Lambda that regularly
        # pulls from CPWD and updates your AllotteesTable.

        # Mock response for demonstration
        return {
            'statusCode': 200,
            'body': json.dumps([
                {
                    "allottee_id": "LSQA001",
                    "employee_id": "PFMS12345",
                    "name": "Smt. Priya Sharma",
                    "quarter_id": "LSL-C-123",
                    "allotment_start_date": "2023-01-15",
                    "allotment_end_date": None,
                    "status": "OCCUPIED",
                    "last_updated": "2025-06-23T10:00:00Z"
                },
                {
                    "allottee_id": "LSQA003",
                    "employee_id": "PFMS54321",
                    "name": "Shri. Rahul Kumar",
                    "quarter_id": "LSL-A-401",
                    "allotment_start_date": "2024-03-01",
                    "allotment_end_date": "2025-06-20", # Recently vacated
                    "status": "VACATED",
                    "last_updated": "2025-06-20T15:30:00Z"
                }
            ])
        }

    elif http_method == 'POST' and path == '/v1/allottees/status-updates':
        # Logic for POST /v1/allottees/status-updates (CPWD pushing updates to us)
        try:
            body = json.loads(event['body'])
            updates = body.get('updates', [])

            for update in updates:
                allottee_id = update.get('allottee_id')
                quarter_id = update.get('quarter_id')
                employee_id = update.get('employee_id')
                status = update.get('status')
                effective_date = update.get('effective_date') # YYYY-MM-DD

                if not all([allottee_id, quarter_id, status, effective_date]):
                    return {
                        'statusCode': 400,
                        'body': json.dumps({'message': 'Missing required fields in update.'})
                    }

                item = {
                    'quarter_id': quarter_id, # Primary Key
                    'allottee_id': allottee_id,
                    'employee_id': employee_id,
                    'status': status,
                    'effective_date': effective_date,
                    'last_updated': datetime.now().isoformat() + 'Z'
                }

                # Logic to handle different statuses (OCCUPIED, VACATED, TRANSFERRED)
                # This is where you manage the 'allotment_start_date' and 'allotment_end_date'
                # in your AllotteesTable.

                # Example: If status is VACATED or TRANSFERRED, set allotment_end_date
                if status in ['VACATED', 'TRANSFERRED']:
                    # Retrieve existing record to preserve start date if needed
                    existing_item = allottees_table.get_item(Key={'quarter_id': quarter_id}).get('Item')
                    if existing_item:
                        item['allotment_start_date'] = existing_item.get('allotment_start_date')
                    item['allotment_end_date'] = effective_date
                elif status == 'OCCUPIED':
                    item['allotment_start_date'] = effective_date
                    item['allotment_end_date'] = None # Currently occupied

                allottees_table.put_item(Item=item)

            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Allottee status updates processed successfully.'})
            }
        except Exception as e:
            print(f"Error processing status update: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps({'message': 'Internal Server Error', 'error': str(e)})
            }

    return {
        'statusCode': 404,
        'body': json.dumps({'message': 'Not Found'})
    }