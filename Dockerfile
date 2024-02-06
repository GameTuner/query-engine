FROM python:3.10-slim
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt
COPY ./queryengine /app/queryengine
COPY ./tests /app/tests
RUN pytest /app/tests

ENV GCP_PROJECT_ID=''
ENV UVICORN_PORT=8080
ENV JSON_LOGS=1
ENV SERVICE_SUFFIX=''

WORKDIR /app
CMD ["uvicorn", "queryengine.main:app", "--host", "0.0.0.0"]
