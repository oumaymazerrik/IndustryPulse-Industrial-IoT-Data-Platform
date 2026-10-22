FROM python:3.11-slim

WORKDIR /app

COPY ml/inference_worker/requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY ml/inference_worker/worker.py ./worker.py

CMD ["python", "-u", "worker.py"]
