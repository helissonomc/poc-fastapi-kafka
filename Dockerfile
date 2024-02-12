FROM tiangolo/uvicorn-gunicorn-fastapi:python3.9


COPY ./app /app
FROM python:3.10

WORKDIR /code

RUN pip install fastapi uvicorn kafka-python

COPY ./app .

CMD ["uvicorn", "main:app", "--reload", "--host", "0.0.0.0", "--port", "8000"]