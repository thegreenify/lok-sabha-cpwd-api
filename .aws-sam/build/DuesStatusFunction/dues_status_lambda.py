import json
import os

import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
allottees_table = dynamodb.Table(os.environ['ALLOTTEES_TABLE_NAME'])
water_bills_table = dynamodb.Table(os.environ['WATER_BILLS_TABLE_NAME'])
payment_statuses_table = dynamodb.Table(os.environ['PAYMENT_STATUSES_TABLE_NAME'])

def lambda_handler(event, context):
    try:
        employee_id = event['pathParameters'].get('employee_id')

        if not employee_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Employee ID is required.'})
            }

        # 1. Get allottee info using GSI on employee_id
        response_allottee = allottees_table.query(
            IndexName='employee_id-index',
            KeyConditionExpression=Key('employee_id').eq(employee_id)
        )
        allottee_info = response_allottee['Items'][0] if response_allottee['Items'] else None

        if not allottee_info:
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'Allottee not found.'})
            }

        allottee_id = allottee_info['allottee_id']
        quarter_id = allottee_info['quarter_id']

        # 2. Query WaterBillsTable for all bills for this allottee
        bills_response = water_bills_table.query(
            KeyConditionExpression=Key('allottee_id').eq(allottee_id)
        )
        bills = bills_response.get('Items', [])

        # 3. Query PaymentStatusesTable for all confirmed payments for this allottee
        payments_response = payment_statuses_table.query(
            KeyConditionExpression=Key('employee_id').eq(employee_id)
        )
        payments = payments_response.get('Items', [])

        # 4. Calculate actual dues
        total_billed = sum(b.get('amount_inr', 0) for b in bills)
        total_paid = sum(p.get('amount_deducted_inr', 0) for p in payments if p.get('status') == 'SUCCESS')

        pending_amount = round(total_billed - total_paid, 2)

        dues_status = "CLEARED" if pending_amount <= 0 else "PENDING"
        pending_months = []
        last_paid_month = None

        if payments:
            successful_payments = [p for p in payments if p.get('status') == 'SUCCESS']
            if successful_payments:
                last_paid_month = max(p.get('billing_month') for p in successful_payments)

        if pending_amount > 0:
            # Find pending months by comparing bills and payments
            for bill in bills:
                bill_month = bill['billing_month']
                bill_amount = bill['amount_inr']
                paid_for_month = sum(p.get('amount_deducted_inr', 0) for p in payments
                                   if p.get('billing_month') == bill_month and p.get('status') == 'SUCCESS')
                if paid_for_month < bill_amount:
                    pending_months.append(bill_month)

        response = {
            'employee_id': employee_id,
            'quarter_id': quarter_id,
            'allottee_id': allottee_id,
            'dues_status': dues_status,
            'total_billed': total_billed,
            'total_paid': total_paid,
            'pending_amount': pending_amount,
            'pending_months': sorted(pending_months) if pending_months else [],
            'last_paid_month': last_paid_month
        }

        return {
            'statusCode': 200,
            'body': json.dumps(response)
        }

    except Exception as e:
        print(f"Error checking dues status: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Internal Server Error', 'error': str(e)})
        }