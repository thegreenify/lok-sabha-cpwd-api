import json
import boto3
import os
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
payment_statuses_table = dynamodb.Table(os.environ['PAYMENT_STATUSES_TABLE_NAME'])
water_bills_table = dynamodb.Table(os.environ['WATER_BILLS_TABLE_NAME'])

def lambda_handler(event, context):
    try:
        body = json.loads(event['body'])
        billing_month = body.get('billing_month')
        job_id = body.get('job_id')
        results = body.get('results', [])

        if not billing_month or not results:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing billing_month or results in request body.'})
            }

        for result in results:
            employee_id = result.get('employee_id')
            amount_deducted_inr = result.get('amount_deducted_inr')
            status = result.get('status')
            failure_reason = result.get('failure_reason')

            if not all([employee_id, amount_deducted_inr is not None, status]):
                print(f"Skipping malformed result: {result}")
                continue

            # Update PaymentStatusesTable
            # from boto3.dynamodb.conditions import Attr  # Import Attr for safe attribute handling
            payment_statuses_table.put_item(
                Item={
                    'employee_id': Attr('employee_id').eq(employee_id),
                    'billing_month': Attr('billing_month').eq(billing_month),
                    'job_id': Attr('job_id').eq(job_id),
                    'amount_deducted_inr': Attr('amount_deducted_inr').eq(amount_deducted_inr),
                    'status': Attr('status').eq(status),
                    'failure_reason': Attr('failure_reason').eq(failure_reason),
                    'confirmed_at': Attr('confirmed_at').eq(datetime.now().isoformat() + 'Z')
                }
            )

            print(f"Confirmed payment for {employee_id} ({billing_month}): {status}")

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Payment confirmations processed successfully.'})
        }
    except Exception as e:
        print(f"Error processing payment confirmation: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Internal Server Error', 'error': str(e)})
        }