FROM python:3.9
WORKDIR /app

FROM python:3.9
WORKDIR /app

# Install dependencies directly
RUN pip install --no-cache-dir boto3 watchdog requests

# Copy your application code (adjust if needed)
COPY . .

CMD ["python", "/upload_script/upload_script.py"]

