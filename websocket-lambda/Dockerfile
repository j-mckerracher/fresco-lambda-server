# Use the official AWS Lambda Python 3.12 base image
FROM public.ecr.aws/lambda/python:3.12

# Copy function code
COPY websocket-lambda.py ${LAMBDA_TASK_ROOT}

# Copy requirements file and install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

RUN pip list

# Set the CMD to your handler
CMD ["websocket-lambda.lambda_handler"]
