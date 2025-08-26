FROM public.ecr.aws/lambda/python:3.10

COPY requirements.txt .
COPY *.py .

RUN pip install -r requirements.txt

CMD ["index.handler"]