FROM python:3.9 

WORKDIR /src 

COPY /src . 

RUN pip install -r requirements.txt 

ENV PYTHONPATH=/src

CMD ["python", "weatherapi/pipeline/weatherapi_pipeline.py"]
