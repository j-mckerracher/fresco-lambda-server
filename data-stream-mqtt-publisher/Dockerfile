# Use the official AWS Lambda Python 3.9 base image
FROM public.ecr.aws/lambda/python:3.9

# Set working directory
WORKDIR ${LAMBDA_TASK_ROOT}

# Install minimal system dependencies
RUN yum update -y && \
    yum install -y \
        postgresql-devel \
        gcc \
        python3-devel && \
    yum clean all && \
    rm -rf /var/cache/yum

# Copy function code
COPY server.py ${LAMBDA_TASK_ROOT}

# Copy requirements file and install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt --target "${LAMBDA_TASK_ROOT}" --no-warn-script-location

# Set the CMD to your handler
CMD ["server.lambda_handler"]