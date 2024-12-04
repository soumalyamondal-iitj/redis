import json
import math


def lambda_handler(event, context):
    
    print("Received Event:", json.dumps(event, indent=2))
    
    try:
        # Validate input keys
        if 'a' not in event or 'b' not in event or 'op' not in event:
            return {
                'statusCode': 400,
                'body': "400 Invalid Input: Missing required values 'a', 'b', or 'op'"
            }

        # Validate input values
        a = event['a']
        b = event['b']
        op = event['op']
        
        if any(math.isnan(x) for x in [a, b]):
            return "400 Invalid Operand"

        # Perform the operation based on the operator
        if op in ('+', 'add'):
            result = add(a, b)
            print(f"The summation of {a} and {b} is {result}")
        elif op in ('-', 'sub'):
            result = subtract(a, b)
            print(f"The subtraction of {a} and {b} is {result}")
        elif op in ('*', 'mul'):
            result = multiply(a, b)
            print(f"The multiplication of {a} and {b} is {result}")
        elif op in ('/', 'div'):
            if b == 0:
                return {
                    'statusCode': 400,
                    'body': "400 Divide by Zero"
                }
            else:
                result = divide(a, b)
                print(f"The division of {a} and {b} is {result}")
        else:
            return {
                'statusCode': 400,
                'body': "400 Invalid Operator"
            }

        # Return the result
        return {
            'statusCode': 200,
            'body': f"Result: {result}"
        }
    except Exception as e:
        return {
            'statusCode': 400,
            'body': str(e)
        }
       
       
# Python program for simple calculator
# Function to add two numbers
def add(num1, num2):
    return num1 + num2
 
# Function to subtract two numbers
def subtract(num1, num2):
    return num1 - num2
 
# Function to multiply two numbers
def multiply(num1, num2):
    return num1 * num2
 
# Function to divide two numbers
def divide(num1, num2):
    return num1 / num2
