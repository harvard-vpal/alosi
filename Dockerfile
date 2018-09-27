FROM python:3.6
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
WORKDIR /app
ENV PYTHONPATH "${PYTHONPATH}:/app"
COPY . .
