FROM python:latest
WORKDIR /app
COPY *.txt ./
RUN pip install -r requirements.txt
COPY . .
CMD ["tail", "-f", "/dev/null"]
