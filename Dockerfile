# Dockerfile
FROM apache/airflow:2.0.2-python3.8

# Constraints compatíveis com a imagem acima (Py 3.8)
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-2.0.2/constraints-3.8.txt

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --constraint ${CONSTRAINT_URL} -r /requirements.txt